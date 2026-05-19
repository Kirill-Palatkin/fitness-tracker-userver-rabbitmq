# Фитнес-трекер

Проект расширен под лабораторные работы с PostgreSQL и MongoDB для варианта 14.

Реализованные endpoint-ы:

- `POST /v1/auth/register`
- `POST /v1/auth/login`
- `GET /v1/users/by-login`
- `GET /v1/users/search`
- `GET /v1/exercises`
- `POST /v1/exercises`
- `POST /v1/workouts`
- `POST /v1/workouts/{workout_id}/exercises`
- `GET /v1/workouts/history`
- `GET /v1/workouts/statistics`
- `GET /v1/mongo/exercises`
- `POST /v1/mongo/exercises`

## Запуск через Docker

```bash
docker compose up --build
```
После запуска API будет доступен по адресу `http://localhost:8080`.
PostgreSQL будет доступен на порту `5432`, MongoDB - на порту `27017`.

Для полной переинициализации PostgreSQL и MongoDB из файлов проекта можно удалить volume-ы и поднять контейнеры заново:

```bash
docker compose down -v
docker compose up --build
```

- `schema.sql` - создание схемы БД, таблиц и индексов
- `data.sql` - тестовые данные
- `queries.sql` - SQL-запросы для основных операций системы
- `optimization.md` - описание оптимизаций и запросов для анализа через EXPLAIN
- `schema_design.md` - описание документной модели MongoDB
- `data.js` - тестовые данные MongoDB
- `queries.js` - CRUD-запросы и aggregation pipeline для MongoDB
- `validation.js` - создание коллекций, индексов и `$jsonSchema`-валидации MongoDB
- `performance_design.md` - стратегия кеширования, rate limiting и анализ производительности

## Схема базы данных

- `users` - пользователи системы. Содержит логин, имя, фамилию, соль и хэш пароля. Первичный ключ: `id`. Поле `login` уникально.
- `sessions` - активные пользовательские сессии. Содержит токен, дату создания и срок действия. Первичный ключ: `token`. Внешний ключ `user_id` ссылается на `users(id)`.
- `exercises` - справочник упражнений. Содержит название, группу мышц, калории в минуту, описание и автора упражнения. Первичный ключ: `id`. Внешний ключ `created_by` ссылается на `users(id)`. Пара `(name, muscle_group)` уникальна.
- `workouts` - тренировки пользователей. Содержит владельца тренировки, название, дату и заметки. Первичный ключ: `id`. Внешний ключ `user_id` ссылается на `users(id)`.
- `workout_exercises` - упражнения внутри конкретной тренировки. Содержит ссылку на тренировку, упражнение, количество подходов, повторений, длительность и позицию в тренировке. Первичный ключ: `id`. Внешние ключи `workout_id` и `exercise_id` ссылаются на `workouts(id)` и `exercises(id)`. Пара `(workout_id, position)` уникальна.

Основные связи между таблицами:

- один пользователь может иметь много сессий;
- один пользователь может создать много упражнений;
- один пользователь может иметь много тренировок;
- одна тренировка может содержать много упражнений через таблицу `workout_exercises`.

## Документная модель MongoDB

Для MongoDB используется база `fitness_tracker_mongo` и коллекции:

- `users` - пользователи системы, профиль пользователя хранится как embedded object, цели пользователя хранятся массивом строк.
- `exercises` - справочник упражнений, оборудование и теги хранятся внутри документа упражнения.
- `workouts` - тренировки пользователей, упражнения внутри тренировки хранятся embedded-массивом `exercises`.

В MongoDB используются references:

- `workouts.user_id` ссылается на `users.user_id`;
- `workouts.exercises.exercise_id` ссылается на `exercises.exercise_id`;
- `exercises.created_by` ссылается на `users.user_id`.

Такой вариант выбран потому, что пользователь и упражнение являются самостоятельными сущностями, а состав конкретной тренировки чаще всего читается вместе с тренировкой.

Подробное описание модели находится в `schema_design.md`.

## MongoDB-запросы

После запуска контейнеров можно открыть MongoDB shell:

```bash
docker compose exec mongo mongosh fitness_tracker_mongo
```

Проверить количество документов:

```javascript
db.users.countDocuments()
db.exercises.countDocuments()
db.workouts.countDocuments()
```

Выполнить CRUD-запросы из файла `queries.js` из папки проекта:

```bash
docker compose exec -T mongo mongosh fitness_tracker_mongo < queries.js
```

В PowerShell:

```powershell
Get-Content .\queries.js | docker compose exec -T mongo mongosh fitness_tracker_mongo
```

## Кеширование и rate limiting

В API реализовано in-memory кеширование для:

- `GET /v1/exercises`
- `GET /v1/workouts/statistics`

Ответы кешируемых эндпоинтов содержат заголовок `X-Cache` со значением `MISS` или `HIT`.

Кеш инвалидируется при изменении связанных данных:

- `POST /v1/exercises` сбрасывает кеш списка упражнений
- `POST /v1/workouts` и `POST /v1/workouts/{workout_id}/exercises` сбрасывают кеш статистики текущего пользователя

Для `POST /v1/auth/login` реализован rate limiting: 5 попыток входа в минуту на один логин. При превышении лимита возвращается `429 Too Many Requests`, в ответ добавляются заголовки `X-RateLimit-Limit`, `X-RateLimit-Remaining` и `X-RateLimit-Reset`.

## Пример сценария работы

Регистрация:

```bash
curl -X POST http://localhost:8080/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{
    "login":"athlete",
    "first_name":"Kirill",
    "last_name":"Palatkin",
    "password":"12345678"
  }'
```

Вход:

```bash
curl -X POST http://localhost:8080/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "login":"athlete",
    "password":"12345678"
  }'
```

Создание упражнения:

```bash
curl -X POST http://localhost:8080/v1/exercises \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "name":"Squats",
    "muscle_group":"Legs",
    "calories_per_minute":8,
    "description":"Упражнения для нижней части тела"
  }'
```

Создание тренировки:

```bash
curl -X POST http://localhost:8080/v1/workouts \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "title":"Утренняя тренировка",
    "planned_date":"2026-03-28",
    "notes":"Сосредоточиться на темпе"
  }'
```

Получение статистики:

```bash
curl "http://localhost:8080/v1/workouts/statistics?from=2026-03-01&to=2026-03-31" \
  -H "Authorization: Bearer <token>"
```

Получение упражнений из MongoDB:

```bash
curl http://localhost:8080/v1/mongo/exercises
```

Создание упражнения в MongoDB:

```bash
curl -X POST http://localhost:8080/v1/mongo/exercises \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "name":"Mongo Demo Exercise",
    "muscle_group":"Core",
    "calories_per_minute":6,
    "description":"Упражнение, сохраненное в MongoDB"
  }'
```

## Тесты

Тесты находятся в `tests/test_fitness_tracker.py`.


## OpenAPI

Контракт API описан в `openapi.yaml`.

## Event-Driven архитектура

Добавлена Event-Driven часть на RabbitMQ.

Файлы:

- `event_driven_design.md` - описание Event-Driven архитектуры, потоков событий, RabbitMQ и CQRS
- `event_catalog.md` - каталог событий
- `events/producer.py` - демонстрационный producer событий
- `events/consumer.py` - consumer событий

RabbitMQ запускается через Docker Compose:

```bash
docker compose up --build
```

Доступен по адресу:

```text
http://localhost:15672
```

Логин и пароль:

```text
guest / guest
```

Проверить публикацию событий:

```bash
docker compose run --rm event-producer
```

Посмотреть, что consumer получил события:

```bash
docker compose logs -f event-consumer
```

Используется exchange `fitness_tracker.events` типа `topic`. Основные события: `UserRegistered`, `ExerciseCreated`, `WorkoutCreated`, `ExerciseAddedToWorkout`.
