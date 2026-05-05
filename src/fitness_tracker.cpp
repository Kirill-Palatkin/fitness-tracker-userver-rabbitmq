#include "fitness_tracker.hpp"

#include <chrono>
#include <cstdint>
#include <mutex>
#include <optional>
#include <regex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>

#include <fmt/format.h>

#include <userver/clients/dns/component.hpp>
#include <userver/components/component.hpp>
#include <userver/crypto/base64.hpp>
#include <userver/crypto/hash.hpp>
#include <userver/crypto/random.hpp>
#include <userver/formats/common/type.hpp>
#include <userver/formats/bson/inline.hpp>
#include <userver/formats/json.hpp>
#include <userver/formats/json/serialize.hpp>
#include <userver/http/content_type.hpp>
#include <userver/server/handlers/http_handler_base.hpp>
#include <userver/server/http/http_status.hpp>
#include <userver/storages/mongo/component.hpp>
#include <userver/storages/mongo/pool.hpp>
#include <userver/storages/postgres/cluster.hpp>
#include <userver/storages/postgres/component.hpp>

namespace fitness_tracker_service {

namespace {

namespace formats_json = userver::formats::json;
namespace formats_bson = userver::formats::bson;
namespace http = userver::server::http;
namespace mongo = userver::storages::mongo;
namespace pg = userver::storages::postgres;

struct PublicUser {
  std::int64_t id{};
  std::string login;
  std::string first_name;
  std::string last_name;
};

struct AuthUser : PublicUser {};

class ApiException final : public std::runtime_error {
 public:
  ApiException(http::HttpStatus status, std::string code, std::string message)
      : std::runtime_error(std::move(message)),
        status_(status),
        code_(std::move(code)) {}

  http::HttpStatus Status() const { return status_; }
  const std::string& Code() const { return code_; }

 private:
  http::HttpStatus status_;
  std::string code_;
};

formats_json::Value BuildUserJson(const PublicUser& user) {
  formats_json::ValueBuilder builder;
  builder["id"] = user.id;
  builder["login"] = user.login;
  builder["first_name"] = user.first_name;
  builder["last_name"] = user.last_name;
  return builder.ExtractValue();
}

bool IsIsoDate(std::string_view value) {
  static const std::regex kDateRegex(R"(^\d{4}-\d{2}-\d{2}$)");
  return std::regex_match(value.begin(), value.end(), kDateRegex);
}

std::string MakeOpaqueToken() {
  return userver::crypto::base64::Base64UrlEncode(
      userver::crypto::GenerateRandomBlock(32),
      userver::crypto::base64::Pad::kWithout);
}

std::string HashPassword(std::string_view password, std::string_view salt) {
  return userver::crypto::hash::Blake2b128(
      fmt::format("{}:{}", salt, password));
}

struct CacheEntry {
  std::string body;
  std::chrono::steady_clock::time_point expires_at;
};

struct RateLimitBucket {
  int count{};
  std::chrono::steady_clock::time_point window_start{};
};

struct RateLimitDecision {
  bool allowed{};
  int limit{};
  int remaining{};
  std::int64_t reset_after_seconds{};
};

class PerformanceState final {
 public:
  std::optional<std::string> GetCachedResponse(std::string_view key) {
    const auto now = std::chrono::steady_clock::now();
    std::lock_guard lock(mutex_);
    const auto it = cache_.find(std::string(key));
    if (it == cache_.end()) {
      return std::nullopt;
    }
    if (it->second.expires_at <= now) {
      cache_.erase(it);
      return std::nullopt;
    }
    return it->second.body;
  }

  void PutCachedResponse(std::string key, std::string body,
                         std::chrono::seconds ttl) {
    std::lock_guard lock(mutex_);
    cache_[std::move(key)] = CacheEntry{
        std::move(body), std::chrono::steady_clock::now() + ttl};
  }

  void InvalidateCache(std::string_view key) {
    std::lock_guard lock(mutex_);
    cache_.erase(std::string(key));
  }

  void InvalidateCachePrefix(std::string_view prefix) {
    std::lock_guard lock(mutex_);
    for (auto it = cache_.begin(); it != cache_.end();) {
      if (it->first.compare(0, prefix.size(), prefix) == 0) {
        it = cache_.erase(it);
      } else {
        ++it;
      }
    }
  }

  RateLimitDecision CheckRateLimit(std::string key, int limit,
                                   std::chrono::seconds window) {
    const auto now = std::chrono::steady_clock::now();
    std::lock_guard lock(mutex_);

    auto& bucket = rate_limits_[std::move(key)];
    if (bucket.window_start.time_since_epoch().count() == 0 ||
        now - bucket.window_start >= window) {
      bucket.window_start = now;
      bucket.count = 0;
    }

    const bool allowed = bucket.count < limit;
    if (allowed) {
      ++bucket.count;
    }

    const auto elapsed =
        std::chrono::duration_cast<std::chrono::seconds>(now -
                                                         bucket.window_start);
    const auto reset_after =
        std::chrono::duration_cast<std::chrono::seconds>(window - elapsed)
            .count();

    return RateLimitDecision{
        allowed,
        limit,
        allowed ? limit - bucket.count : 0,
        reset_after > 0 ? reset_after : 0,
    };
  }

 private:
  std::mutex mutex_;
  std::unordered_map<std::string, CacheEntry> cache_;
  std::unordered_map<std::string, RateLimitBucket> rate_limits_;
};

PerformanceState& GetPerformanceState() {
  static PerformanceState state;
  return state;
}

std::string MakeStatsCacheKey(std::int64_t user_id, std::string_view from,
                              std::string_view to) {
  return fmt::format("postgres:workout_stats:{}:{}:{}", user_id, from, to);
}

std::string MakeStatsCachePrefix(std::int64_t user_id) {
  return fmt::format("postgres:workout_stats:{}:", user_id);
}

class ApiHandlerBase : public userver::server::handlers::HttpHandlerBase {
 public:
  ApiHandlerBase(const userver::components::ComponentConfig& config,
                 const userver::components::ComponentContext& component_context)
      : HttpHandlerBase(config, component_context),
        pg_cluster_(
            component_context
                .FindComponent<userver::components::Postgres>("postgres-db-1")
                .GetCluster()) {}

 protected:
  template <typename Callback>
  std::string ExecuteSafely(const http::HttpRequest& request,
                            Callback&& callback) const {
    try {
      return callback();
    } catch (const ApiException& ex) {
      return ErrorResponse(request, ex.Status(), ex.Code(), ex.what());
    } catch (const std::exception& ex) {
      return ErrorResponse(request, http::HttpStatus::kInternalServerError,
                           "internal_error", ex.what());
    }
  }

  const pg::ClusterPtr& GetPgCluster() const { return pg_cluster_; }

  formats_json::Value ParseJsonBody(const http::HttpRequest& request) const {
    if (request.RequestBody().empty()) {
      return formats_json::FromString("{}");
    }
    try {
      return formats_json::FromString(request.RequestBody());
    } catch (const std::exception&) {
      throw ApiException(http::HttpStatus::kBadRequest, "invalid_json",
                         "Тело запроса должно содержать корректный JSON");
    }
  }

  std::string GetRequiredStringField(const formats_json::Value& json,
                                     std::string_view field_name) const {
    const auto value = json[std::string(field_name)].As<std::string>("");
    if (value.empty()) {
      throw ApiException(http::HttpStatus::kBadRequest, "validation_error",
                         fmt::format("Поле '{}' не должно быть пустым",
                                     field_name));
    }
    return value;
  }

  int GetPositiveIntField(const formats_json::Value& json,
                          std::string_view field_name) const {
    const auto value = json[std::string(field_name)].As<int>(0);
    if (value <= 0) {
      throw ApiException(http::HttpStatus::kBadRequest, "validation_error",
                         fmt::format("Поле '{}' должно быть положительным целым числом",
                                     field_name));
    }
    return value;
  }

  std::string GetRequiredQueryArg(const http::HttpRequest& request,
                                  std::string_view arg_name) const {
    const auto value = std::string(request.GetArg(std::string(arg_name)));
    if (value.empty()) {
      throw ApiException(http::HttpStatus::kBadRequest, "validation_error",
                         fmt::format("Query-параметр '{}' обязателен",
                                     arg_name));
    }
    return value;
  }

  std::int64_t ParseId(std::string_view raw_value,
                       std::string_view field_name) const {
    try {
      const auto parsed = std::stoll(std::string(raw_value));
      if (parsed <= 0) {
        throw std::invalid_argument("non-positive");
      }
      return parsed;
    } catch (const std::exception&) {
      throw ApiException(http::HttpStatus::kBadRequest, "validation_error",
                         fmt::format("'{}' должен быть положительным целым числом",
                                     field_name));
    }
  }

  void ValidateDate(std::string_view date_value,
                    std::string_view field_name) const {
    if (!IsIsoDate(date_value)) {
      throw ApiException(http::HttpStatus::kBadRequest, "validation_error",
                         fmt::format("'{}' должен быть в формате YYYY-MM-DD",
                                     field_name));
    }
  }

  std::string JsonResponse(const http::HttpRequest& request,
                           formats_json::Value value,
                           http::HttpStatus status =
                               http::HttpStatus::kOk) const {
    auto& response = request.GetHttpResponse();
    response.SetStatus(status);
    response.SetContentType(userver::http::content_type::kApplicationJson);
    return formats_json::ToString(value);
  }

  std::string JsonTextResponse(const http::HttpRequest& request,
                               std::string body,
                               http::HttpStatus status =
                                   http::HttpStatus::kOk) const {
    auto& response = request.GetHttpResponse();
    response.SetStatus(status);
    response.SetContentType(userver::http::content_type::kApplicationJson);
    return body;
  }

  void SetCacheHeader(const http::HttpRequest& request,
                      std::string_view value) const {
    request.GetHttpResponse().SetHeader(std::string("X-Cache"),
                                         std::string(value));
  }

  void SetRateLimitHeaders(const http::HttpRequest& request,
                           const RateLimitDecision& decision) const {
    auto& response = request.GetHttpResponse();
    response.SetHeader(std::string("X-RateLimit-Limit"),
                       std::to_string(decision.limit));
    response.SetHeader(std::string("X-RateLimit-Remaining"),
                       std::to_string(decision.remaining));
    response.SetHeader(std::string("X-RateLimit-Reset"),
                       std::to_string(decision.reset_after_seconds));
  }

  std::string JsonResponse(const http::HttpRequest& request,
                           formats_json::ValueBuilder builder,
                           http::HttpStatus status =
                               http::HttpStatus::kOk) const {
    return JsonResponse(request, builder.ExtractValue(), status);
  }

  std::string ErrorResponse(const http::HttpRequest& request,
                            http::HttpStatus status, std::string_view code,
                            std::string_view message) const {
    formats_json::ValueBuilder builder;
    builder["error"] = std::string(code);
    builder["message"] = std::string(message);
    return JsonResponse(request, builder.ExtractValue(), status);
  }

  AuthUser RequireAuth(const http::HttpRequest& request) const {
    const auto auth_header = std::string(request.GetHeader("Authorization"));
    const std::string kBearerPrefix = "Bearer ";
    if (auth_header.compare(0, kBearerPrefix.size(), kBearerPrefix) != 0) {
      throw ApiException(http::HttpStatus::kUnauthorized, "unauthorized",
                         "Требуется Bearer-токен");
    }

    const auto token = auth_header.substr(kBearerPrefix.size());
    if (token.empty()) {
      throw ApiException(http::HttpStatus::kUnauthorized, "unauthorized",
                         "Требуется Bearer-токен");
    }

    const auto result = pg_cluster_->Execute(
        pg::ClusterHostType::kMaster,
        "SELECT u.id, u.login, u.first_name, u.last_name "
        "FROM fitness_tracker.sessions s "
        "JOIN fitness_tracker.users u ON u.id = s.user_id "
        "WHERE s.token = $1 AND s.expires_at > NOW()",
        token);

    if (result.Size() == 0) {
      throw ApiException(http::HttpStatus::kUnauthorized, "unauthorized",
                         "Токен отсутствует или истёк");
    }

    const auto row = result[0];
    return AuthUser{
        row["id"].As<std::int64_t>(),
        row["login"].As<std::string>(),
        row["first_name"].As<std::string>(),
        row["last_name"].As<std::string>(),
    };
  }

  void EnsureWorkoutBelongsToUser(std::int64_t workout_id,
                                  std::int64_t user_id) const {
    const auto result = pg_cluster_->Execute(
        pg::ClusterHostType::kMaster,
        "SELECT id FROM fitness_tracker.workouts "
        "WHERE id = $1 AND user_id = $2",
        workout_id, user_id);
    if (result.Size() == 0) {
      throw ApiException(http::HttpStatus::kNotFound, "workout_not_found",
                         "Тренировка текущего пользователя не найдена");
    }
  }

  void EnsureExerciseExists(std::int64_t exercise_id) const {
    const auto result = pg_cluster_->Execute(
        pg::ClusterHostType::kMaster,
        "SELECT id FROM fitness_tracker.exercises WHERE id = $1",
        exercise_id);
    if (result.Size() == 0) {
      throw ApiException(http::HttpStatus::kNotFound, "exercise_not_found",
                         "Упражнение не найдено");
    }
  }

 private:
  pg::ClusterPtr pg_cluster_;
};

class MongoApiHandlerBase : public ApiHandlerBase {
 public:
  MongoApiHandlerBase(
      const userver::components::ComponentConfig& config,
      const userver::components::ComponentContext& component_context)
      : ApiHandlerBase(config, component_context),
        mongo_pool_(
            component_context
                .FindComponent<userver::components::Mongo>("mongo-db-1")
                .GetPool()) {}

 protected:
  const mongo::PoolPtr& GetMongoPool() const { return mongo_pool_; }

 private:
  mongo::PoolPtr mongo_pool_;
};

class RegisterHandler final : public ApiHandlerBase {
 public:
  static constexpr std::string_view kName = "handler-register";

  using ApiHandlerBase::ApiHandlerBase;

  std::string HandleRequestThrow(
      const http::HttpRequest& request,
      userver::server::request::RequestContext&) const override {
    return ExecuteSafely(request, [&] {
      const auto json = ParseJsonBody(request);
      const auto login = GetRequiredStringField(json, "login");
      const auto first_name = GetRequiredStringField(json, "first_name");
      const auto last_name = GetRequiredStringField(json, "last_name");
      const auto password = GetRequiredStringField(json, "password");

      const auto existing = GetPgCluster()->Execute(
          pg::ClusterHostType::kMaster,
          "SELECT id FROM fitness_tracker.users WHERE login = $1", login);
      if (existing.Size() != 0) {
        throw ApiException(http::HttpStatus::kConflict, "login_taken",
                           "Пользователь с таким логином уже существует");
      }

      const auto salt = MakeOpaqueToken();
      const auto password_hash = HashPassword(password, salt);

      const auto result = GetPgCluster()->Execute(
          pg::ClusterHostType::kMaster,
          "INSERT INTO fitness_tracker.users("
          "login, first_name, last_name, password_salt, password_hash) "
          "VALUES($1, $2, $3, $4, $5) "
          "RETURNING id",
          login, first_name, last_name, salt, password_hash);

      PublicUser user{
          result[0]["id"].As<std::int64_t>(),
          login,
          first_name,
          last_name,
      };

      formats_json::ValueBuilder builder;
      builder["user"] = BuildUserJson(user);
      builder["message"] = "Пользователь успешно зарегистрирован";
      return JsonResponse(request, builder, http::HttpStatus::kCreated);
    });
  }
};

class LoginHandler final : public ApiHandlerBase {
 public:
  static constexpr std::string_view kName = "handler-login";

  using ApiHandlerBase::ApiHandlerBase;

  std::string HandleRequestThrow(
      const http::HttpRequest& request,
      userver::server::request::RequestContext&) const override {
    return ExecuteSafely(request, [&] {
      const auto json = ParseJsonBody(request);
      const auto login = GetRequiredStringField(json, "login");
      const auto password = GetRequiredStringField(json, "password");

      const auto rate_limit = GetPerformanceState().CheckRateLimit(
          "login:" + login, 5, std::chrono::seconds{60});
      SetRateLimitHeaders(request, rate_limit);
      if (!rate_limit.allowed) {
        throw ApiException(http::HttpStatus::kTooManyRequests, "rate_limited",
                           "Слишком много попыток входа. Повторите запрос позже");
      }

      const auto result = GetPgCluster()->Execute(
          pg::ClusterHostType::kMaster,
          "SELECT id, login, first_name, last_name, password_salt, "
          "password_hash "
          "FROM fitness_tracker.users WHERE login = $1",
          login);

      if (result.Size() == 0) {
        throw ApiException(http::HttpStatus::kUnauthorized,
                           "invalid_credentials",
                           "Неверный логин или пароль");
      }

      const auto row = result[0];
      const auto password_salt = row["password_salt"].As<std::string>();
      const auto password_hash = row["password_hash"].As<std::string>();
      if (HashPassword(password, password_salt) != password_hash) {
        throw ApiException(http::HttpStatus::kUnauthorized,
                           "invalid_credentials",
                           "Неверный логин или пароль");
      }

      const auto user = PublicUser{
          row["id"].As<std::int64_t>(),
          row["login"].As<std::string>(),
          row["first_name"].As<std::string>(),
          row["last_name"].As<std::string>(),
      };

      const auto token = MakeOpaqueToken();
      GetPgCluster()->Execute(
          pg::ClusterHostType::kMaster,
          "INSERT INTO fitness_tracker.sessions(user_id, token, expires_at) "
          "VALUES($1, $2, NOW() + INTERVAL '7 days')",
          user.id, token);

      formats_json::ValueBuilder builder;
      builder["token"] = token;
      builder["user"] = BuildUserJson(user);
      return JsonResponse(request, builder);
    });
  }
};

class UserByLoginHandler final : public ApiHandlerBase {
 public:
  static constexpr std::string_view kName = "handler-user-by-login";

  using ApiHandlerBase::ApiHandlerBase;

  std::string HandleRequestThrow(
      const http::HttpRequest& request,
      userver::server::request::RequestContext&) const override {
    return ExecuteSafely(request, [&] {
      const auto login = GetRequiredQueryArg(request, "login");
      const auto result = GetPgCluster()->Execute(
          pg::ClusterHostType::kMaster,
          "SELECT id, login, first_name, last_name "
          "FROM fitness_tracker.users WHERE login = $1",
          login);

      if (result.Size() == 0) {
        throw ApiException(http::HttpStatus::kNotFound, "user_not_found",
                           "Пользователь не найден");
      }

      const auto row = result[0];
      PublicUser user{
          row["id"].As<std::int64_t>(),
          row["login"].As<std::string>(),
          row["first_name"].As<std::string>(),
          row["last_name"].As<std::string>(),
      };

      formats_json::ValueBuilder builder;
      builder["user"] = BuildUserJson(user);
      return JsonResponse(request, builder);
    });
  }
};

class UserSearchHandler final : public ApiHandlerBase {
 public:
  static constexpr std::string_view kName = "handler-user-search";

  using ApiHandlerBase::ApiHandlerBase;

  std::string HandleRequestThrow(
      const http::HttpRequest& request,
      userver::server::request::RequestContext&) const override {
    return ExecuteSafely(request, [&] {
      const auto query = GetRequiredQueryArg(request, "query");
      const auto pattern = "%" + query + "%";

      const auto result = GetPgCluster()->Execute(
          pg::ClusterHostType::kMaster,
          "SELECT id, login, first_name, last_name "
          "FROM fitness_tracker.users "
          "WHERE first_name ILIKE $1 OR last_name ILIKE $1 "
          "OR (first_name || ' ' || last_name) ILIKE $1 "
          "ORDER BY last_name, first_name LIMIT 20",
          pattern);

      formats_json::ValueBuilder users(
          userver::formats::common::Type::kArray);
      for (const auto& row : result) {
        PublicUser user{
            row["id"].As<std::int64_t>(),
            row["login"].As<std::string>(),
            row["first_name"].As<std::string>(),
            row["last_name"].As<std::string>(),
        };
        users.PushBack(BuildUserJson(user));
      }

      formats_json::ValueBuilder builder;
      builder["users"] = users.ExtractValue();
      return JsonResponse(request, builder);
    });
  }
};

class ExerciseCreateHandler final : public ApiHandlerBase {
 public:
  static constexpr std::string_view kName = "handler-exercise-create";

  using ApiHandlerBase::ApiHandlerBase;

  std::string HandleRequestThrow(
      const http::HttpRequest& request,
      userver::server::request::RequestContext&) const override {
    return ExecuteSafely(request, [&] {
      const auto auth_user = RequireAuth(request);
      const auto json = ParseJsonBody(request);
      const auto name = GetRequiredStringField(json, "name");
      const auto muscle_group = GetRequiredStringField(json, "muscle_group");
      const auto calories_per_minute =
          GetPositiveIntField(json, "calories_per_minute");
      const auto description = json["description"].As<std::string>("");

      const auto result = GetPgCluster()->Execute(
          pg::ClusterHostType::kMaster,
          "INSERT INTO fitness_tracker.exercises("
          "name, muscle_group, calories_per_minute, description, created_by) "
          "VALUES($1, $2, $3, $4, $5) "
          "RETURNING id",
          name, muscle_group, calories_per_minute, description, auth_user.id);

      formats_json::ValueBuilder exercise;
      exercise["id"] = result[0]["id"].As<std::int64_t>();
      exercise["exercise_id"] = result[0]["id"].As<std::int64_t>();
      exercise["name"] = name;
      exercise["muscle_group"] = muscle_group;
      exercise["calories_per_minute"] = calories_per_minute;
      exercise["description"] = description;
      exercise["created_by"] = auth_user.id;
      GetPerformanceState().InvalidateCache("postgres:exercises:list");
      return JsonResponse(request, exercise, http::HttpStatus::kCreated);
    });
  }
};

class ExerciseListHandler final : public ApiHandlerBase {
 public:
  static constexpr std::string_view kName = "handler-exercise-list";

  using ApiHandlerBase::ApiHandlerBase;

  std::string HandleRequestThrow(
      const http::HttpRequest& request,
      userver::server::request::RequestContext&) const override {
    return ExecuteSafely(request, [&] {
      constexpr std::string_view kCacheKey = "postgres:exercises:list";
      if (const auto cached =
              GetPerformanceState().GetCachedResponse(kCacheKey)) {
        SetCacheHeader(request, "HIT");
        return JsonTextResponse(request, *cached);
      }

      const auto result = GetPgCluster()->Execute(
          pg::ClusterHostType::kMaster,
          "SELECT id, name, muscle_group, calories_per_minute, "
          "COALESCE(description, '') AS description, "
          "COALESCE(created_by, 0) AS created_by "
          "FROM fitness_tracker.exercises "
          "ORDER BY name, id");

      formats_json::ValueBuilder exercises(
          userver::formats::common::Type::kArray);
      for (const auto& row : result) {
        formats_json::ValueBuilder exercise;
        exercise["id"] = row["id"].As<std::int64_t>();
        exercise["name"] = row["name"].As<std::string>();
        exercise["muscle_group"] = row["muscle_group"].As<std::string>();
        exercise["calories_per_minute"] =
            row["calories_per_minute"].As<int>();
        exercise["description"] = row["description"].As<std::string>();
        const auto created_by = row["created_by"].As<std::int64_t>();
        if (created_by > 0) {
          exercise["created_by"] = created_by;
        }
        exercises.PushBack(exercise.ExtractValue());
      }

      formats_json::ValueBuilder builder;
      builder["exercises"] = exercises.ExtractValue();
      const auto body = formats_json::ToString(builder.ExtractValue());
      GetPerformanceState().PutCachedResponse(std::string(kCacheKey), body,
                                              std::chrono::seconds{60});
      SetCacheHeader(request, "MISS");
      return JsonTextResponse(request, body);
    });
  }
};

class MongoExerciseCreateHandler final : public MongoApiHandlerBase {
 public:
  static constexpr std::string_view kName = "handler-mongo-exercise-create";

  using MongoApiHandlerBase::MongoApiHandlerBase;

  std::string HandleRequestThrow(
      const http::HttpRequest& request,
      userver::server::request::RequestContext&) const override {
    return ExecuteSafely(request, [&] {
      const auto auth_user = RequireAuth(request);
      const auto json = ParseJsonBody(request);
      const auto name = GetRequiredStringField(json, "name");
      const auto muscle_group = GetRequiredStringField(json, "muscle_group");
      const auto calories_per_minute =
          GetPositiveIntField(json, "calories_per_minute");
      const auto description = json["description"].As<std::string>("");

      auto exercises = GetMongoPool()->GetCollection("exercises");
      exercises.InsertOne(formats_bson::MakeDoc(
          "name", name,
          "muscle_group", muscle_group,
          "calories_per_minute", calories_per_minute,
          "description", description,
          "created_by", auth_user.id,
          "source", "api"));

      formats_json::ValueBuilder builder;
      builder["name"] = name;
      builder["muscle_group"] = muscle_group;
      builder["calories_per_minute"] = calories_per_minute;
      builder["description"] = description;
      builder["created_by"] = auth_user.id;
      builder["storage"] = "mongodb";
      return JsonResponse(request, builder, http::HttpStatus::kCreated);
    });
  }
};

class MongoExerciseListHandler final : public MongoApiHandlerBase {
 public:
  static constexpr std::string_view kName = "handler-mongo-exercise-list";

  using MongoApiHandlerBase::MongoApiHandlerBase;

  std::string HandleRequestThrow(
      const http::HttpRequest& request,
      userver::server::request::RequestContext&) const override {
    return ExecuteSafely(request, [&] {
      auto collection = GetMongoPool()->GetCollection("exercises");
      auto cursor = collection.Find(formats_bson::MakeDoc());

      formats_json::ValueBuilder exercises(
          userver::formats::common::Type::kArray);
      for (const auto& doc : cursor) {
        formats_json::ValueBuilder exercise;
        exercise["exercise_id"] = doc["exercise_id"].As<int>(0);
        exercise["name"] = doc["name"].As<std::string>("");
        exercise["muscle_group"] = doc["muscle_group"].As<std::string>("");
        exercise["calories_per_minute"] =
            doc["calories_per_minute"].As<int>(0);
        exercise["description"] = doc["description"].As<std::string>("");
        exercise["created_by"] = doc["created_by"].As<int>(0);
        exercises.PushBack(exercise.ExtractValue());
      }

      formats_json::ValueBuilder builder;
      builder["storage"] = "mongodb";
      builder["exercises"] = exercises.ExtractValue();
      return JsonResponse(request, builder);
    });
  }
};

class WorkoutCreateHandler final : public ApiHandlerBase {
 public:
  static constexpr std::string_view kName = "handler-workout-create";

  using ApiHandlerBase::ApiHandlerBase;

  std::string HandleRequestThrow(
      const http::HttpRequest& request,
      userver::server::request::RequestContext&) const override {
    return ExecuteSafely(request, [&] {
      const auto auth_user = RequireAuth(request);
      const auto json = ParseJsonBody(request);
      const auto title = GetRequiredStringField(json, "title");
      const auto planned_date = GetRequiredStringField(json, "planned_date");
      ValidateDate(planned_date, "planned_date");
      const auto notes = json["notes"].As<std::string>("");

      const auto result = GetPgCluster()->Execute(
          pg::ClusterHostType::kMaster,
          "INSERT INTO fitness_tracker.workouts(user_id, title, planned_date, "
          "notes) "
          "VALUES($1, $2, $3::date, $4) "
          "RETURNING id, title, to_char(planned_date, 'YYYY-MM-DD') AS "
          "planned_date, COALESCE(notes, '') AS notes",
          auth_user.id, title, planned_date, notes);

      const auto row = result[0];
      formats_json::ValueBuilder builder;
      builder["id"] = row["id"].As<std::int64_t>();
      builder["workout_id"] = row["id"].As<std::int64_t>();
      builder["user_id"] = auth_user.id;
      builder["title"] = row["title"].As<std::string>();
      builder["planned_date"] = row["planned_date"].As<std::string>();
      builder["notes"] = row["notes"].As<std::string>();
      GetPerformanceState().InvalidateCachePrefix(
          MakeStatsCachePrefix(auth_user.id));
      return JsonResponse(request, builder, http::HttpStatus::kCreated);
    });
  }
};

class WorkoutExerciseAddHandler final : public ApiHandlerBase {
 public:
  static constexpr std::string_view kName = "handler-workout-exercise-add";

  using ApiHandlerBase::ApiHandlerBase;

  std::string HandleRequestThrow(
      const http::HttpRequest& request,
      userver::server::request::RequestContext&) const override {
    return ExecuteSafely(request, [&] {
      const auto auth_user = RequireAuth(request);
      const auto workout_id = ParseId(request.GetPathArg("workout_id"),
                                      "workout_id");
      EnsureWorkoutBelongsToUser(workout_id, auth_user.id);

      const auto json = ParseJsonBody(request);
      const auto exercise_id =
          static_cast<std::int64_t>(GetPositiveIntField(json, "exercise_id"));
      EnsureExerciseExists(exercise_id);

      const auto sets = GetPositiveIntField(json, "sets");
      const auto reps = GetPositiveIntField(json, "reps");
      const auto duration_minutes =
          GetPositiveIntField(json, "duration_minutes");

      const auto result = GetPgCluster()->Execute(
          pg::ClusterHostType::kMaster,
          "INSERT INTO fitness_tracker.workout_exercises("
          "workout_id, exercise_id, sets, reps, duration_minutes, position) "
          "VALUES("
          "$1, $2, $3, $4, $5, "
          "COALESCE((SELECT MAX(position) + 1 FROM "
          "fitness_tracker.workout_exercises WHERE workout_id = $1), 1)) "
          "RETURNING id",
          workout_id, exercise_id, sets, reps, duration_minutes);

      formats_json::ValueBuilder builder;
      builder["id"] = result[0]["id"].As<std::int64_t>();
      builder["workout_id"] = workout_id;
      builder["exercise_id"] = exercise_id;
      builder["sets"] = sets;
      builder["reps"] = reps;
      builder["duration_minutes"] = duration_minutes;
      GetPerformanceState().InvalidateCachePrefix(
          MakeStatsCachePrefix(auth_user.id));
      return JsonResponse(request, builder, http::HttpStatus::kCreated);
    });
  }
};

class WorkoutHistoryHandler final : public ApiHandlerBase {
 public:
  static constexpr std::string_view kName = "handler-workout-history";

  using ApiHandlerBase::ApiHandlerBase;

  std::string HandleRequestThrow(
      const http::HttpRequest& request,
      userver::server::request::RequestContext&) const override {
    return ExecuteSafely(request, [&] {
      const auto auth_user = RequireAuth(request);
      const auto from = std::string(request.GetArg("from"));
      const auto to = std::string(request.GetArg("to"));
      if (!from.empty()) ValidateDate(from, "from");
      if (!to.empty()) ValidateDate(to, "to");

      const auto workouts = [&]() {
        if (!from.empty() && !to.empty()) {
          return GetPgCluster()->Execute(
              pg::ClusterHostType::kMaster,
              "SELECT id, title, to_char(planned_date, 'YYYY-MM-DD') AS "
              "planned_date, COALESCE(notes, '') AS notes "
              "FROM fitness_tracker.workouts "
              "WHERE user_id = $1 AND planned_date BETWEEN $2::date "
              "AND $3::date "
              "ORDER BY planned_date DESC, id DESC",
              auth_user.id, from, to);
        }
        if (!from.empty()) {
          return GetPgCluster()->Execute(
              pg::ClusterHostType::kMaster,
              "SELECT id, title, to_char(planned_date, 'YYYY-MM-DD') AS "
              "planned_date, COALESCE(notes, '') AS notes "
              "FROM fitness_tracker.workouts "
              "WHERE user_id = $1 AND planned_date >= $2::date "
              "ORDER BY planned_date DESC, id DESC",
              auth_user.id, from);
        }
        if (!to.empty()) {
          return GetPgCluster()->Execute(
              pg::ClusterHostType::kMaster,
              "SELECT id, title, to_char(planned_date, 'YYYY-MM-DD') AS "
              "planned_date, COALESCE(notes, '') AS notes "
              "FROM fitness_tracker.workouts "
              "WHERE user_id = $1 AND planned_date <= $2::date "
              "ORDER BY planned_date DESC, id DESC",
              auth_user.id, to);
        }
        return GetPgCluster()->Execute(
            pg::ClusterHostType::kMaster,
            "SELECT id, title, to_char(planned_date, 'YYYY-MM-DD') AS "
            "planned_date, COALESCE(notes, '') AS notes "
            "FROM fitness_tracker.workouts "
            "WHERE user_id = $1 "
            "ORDER BY planned_date DESC, id DESC",
            auth_user.id);
      }();

      formats_json::ValueBuilder workouts_json(
          userver::formats::common::Type::kArray);
      for (const auto& workout_row : workouts) {
        const auto workout_id = workout_row["id"].As<std::int64_t>();
        const auto entries = GetPgCluster()->Execute(
            pg::ClusterHostType::kMaster,
            "SELECT we.id, e.id AS exercise_id, e.name, e.muscle_group, "
            "we.sets, we.reps, we.duration_minutes, "
            "ROUND((we.duration_minutes * e.calories_per_minute)::numeric, 2)"
            "::double precision AS calories_burned "
            "FROM fitness_tracker.workout_exercises we "
            "JOIN fitness_tracker.exercises e ON e.id = we.exercise_id "
            "WHERE we.workout_id = $1 "
            "ORDER BY we.position, we.id",
            workout_id);

        formats_json::ValueBuilder exercises(
            userver::formats::common::Type::kArray);
        int total_duration = 0;
        double total_calories = 0.0;
        for (const auto& entry_row : entries) {
          const auto duration = entry_row["duration_minutes"].As<int>();
          const auto calories = entry_row["calories_burned"].As<double>();
          total_duration += duration;
          total_calories += calories;

          formats_json::ValueBuilder exercise;
          exercise["id"] = entry_row["id"].As<std::int64_t>();
          exercise["exercise_id"] =
              entry_row["exercise_id"].As<std::int64_t>();
          exercise["name"] = entry_row["name"].As<std::string>();
          exercise["muscle_group"] =
              entry_row["muscle_group"].As<std::string>();
          exercise["sets"] = entry_row["sets"].As<int>();
          exercise["reps"] = entry_row["reps"].As<int>();
          exercise["duration_minutes"] = duration;
          exercise["calories_burned"] = calories;
          exercises.PushBack(exercise.ExtractValue());
        }

        formats_json::ValueBuilder workout;
        workout["id"] = workout_id;
        workout["title"] = workout_row["title"].As<std::string>();
        workout["planned_date"] =
            workout_row["planned_date"].As<std::string>();
        workout["notes"] = workout_row["notes"].As<std::string>();
        workout["total_duration_minutes"] = total_duration;
        workout["total_calories_burned"] = total_calories;
        workout["exercises"] = exercises.ExtractValue();
        workouts_json.PushBack(workout.ExtractValue());
      }

      formats_json::ValueBuilder builder;
      builder["workouts"] = workouts_json.ExtractValue();
      return JsonResponse(request, builder);
    });
  }
};

class WorkoutStatsHandler final : public ApiHandlerBase {
 public:
  static constexpr std::string_view kName = "handler-workout-statistics";

  using ApiHandlerBase::ApiHandlerBase;

  std::string HandleRequestThrow(
      const http::HttpRequest& request,
      userver::server::request::RequestContext&) const override {
    return ExecuteSafely(request, [&] {
      const auto auth_user = RequireAuth(request);
      const auto from = GetRequiredQueryArg(request, "from");
      const auto to = GetRequiredQueryArg(request, "to");
      ValidateDate(from, "from");
      ValidateDate(to, "to");

      const auto cache_key = MakeStatsCacheKey(auth_user.id, from, to);
      if (const auto cached =
              GetPerformanceState().GetCachedResponse(cache_key)) {
        SetCacheHeader(request, "HIT");
        return JsonTextResponse(request, *cached);
      }

      const auto result = GetPgCluster()->Execute(
          pg::ClusterHostType::kMaster,
          "SELECT "
          "COUNT(DISTINCT w.id) AS workout_count, "
          "COUNT(we.id) AS exercise_entries_count, "
          "COALESCE(SUM(we.duration_minutes), 0) AS total_minutes, "
          "COALESCE(ROUND(SUM(we.duration_minutes * e.calories_per_minute)"
          "::numeric, 2), 0)::double precision AS total_calories "
          "FROM fitness_tracker.workouts w "
          "LEFT JOIN fitness_tracker.workout_exercises we "
          "ON we.workout_id = w.id "
          "LEFT JOIN fitness_tracker.exercises e ON e.id = we.exercise_id "
          "WHERE w.user_id = $1 AND w.planned_date BETWEEN $2::date AND "
          "$3::date",
          auth_user.id, from, to);

      const auto row = result[0];
      formats_json::ValueBuilder builder;
      builder["from"] = from;
      builder["to"] = to;
      builder["workout_count"] = row["workout_count"].As<std::int64_t>();
      builder["exercise_entries_count"] =
          row["exercise_entries_count"].As<std::int64_t>();
      builder["total_exercise_minutes"] =
          row["total_minutes"].As<std::int64_t>();
      builder["total_calories_burned"] = row["total_calories"].As<double>();
      const auto body = formats_json::ToString(builder.ExtractValue());
      GetPerformanceState().PutCachedResponse(cache_key, body,
                                              std::chrono::seconds{30});
      SetCacheHeader(request, "MISS");
      return JsonTextResponse(request, body);
    });
  }
};

}

void AppendFitnessTracker(userver::components::ComponentList& component_list) {
  component_list.Append<RegisterHandler>();
  component_list.Append<LoginHandler>();
  component_list.Append<UserByLoginHandler>();
  component_list.Append<UserSearchHandler>();
  component_list.Append<ExerciseCreateHandler>();
  component_list.Append<ExerciseListHandler>();
  component_list.Append<MongoExerciseCreateHandler>();
  component_list.Append<MongoExerciseListHandler>();
  component_list.Append<WorkoutCreateHandler>();
  component_list.Append<WorkoutExerciseAddHandler>();
  component_list.Append<WorkoutHistoryHandler>();
  component_list.Append<WorkoutStatsHandler>();
  component_list.Append<userver::components::Postgres>("postgres-db-1");
  component_list.Append<userver::components::Mongo>("mongo-db-1");
  component_list.Append<userver::clients::dns::Component>();
}

}
