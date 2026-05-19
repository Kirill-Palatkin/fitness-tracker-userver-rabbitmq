# Event-Driven архитектура фитнес-трекера

## Команды и события

Команды - действия пользователя или API, которые изменяют состояние системы:

- `RegisterUser` - регистрация нового пользователя
- `CreateExercise` - создание упражнения
- `CreateWorkout` - создание тренировки
- `AddExerciseToWorkout` - добавление упражнения в тренировку

После успешного выполнения команд появляются доменные события:

- `UserRegistered`
- `ExerciseCreated`
- `WorkoutCreated`
- `ExerciseAddedToWorkout`

Событие фиксирует уже произошедший факт. Например, команда `CreateWorkout` просит создать тренировку, а событие `WorkoutCreated` сообщает другим компонентам, что тренировка уже создана.

## Компоненты системы

Producer'ы событий:

- `fitness-tracker-api` - REST API, который выполняет команды пользователя
- `fitness-tracker-event-producer` - producer, публикующий события в RabbitMQ

Consumer'ы событий:

- `fitness-tracker-audit-consumer` - consumer, читающий события и выводящий их в лог
- `notification-service` - consumer, который может отправлять уведомления о тренировках
- `statistics-read-model-service` - consumer, который может обновлять read-модель статистики

В текущей реализации реализованы:

- RabbitMQ;
- демонстрационный producer `events/producer.py`;
- consumer `events/consumer.py`.

## Брокер сообщений

Используется exchange:

- имя: `fitness_tracker.events`;
- тип: `topic`;
- durable: `true`.

Основная очередь:

- имя: `fitness_tracker.audit`;
- binding key: `fitness.#`.

Routing keys:

- `fitness.user.registered`
- `fitness.exercise.created`
- `fitness.workout.created`
- `fitness.workout.exercise_added`

Topic exchange выбран потому что события можно маршрутизировать по доменным областям (audit-service может подписаться на `fitness.#`, сервис статистики только на `fitness.workout.#`).

## Формат сообщений

Все события публикуются в JSON.

Общая структура:

```json
{
  "event_id": "uuid",
  "event_type": "WorkoutCreated",
  "occurred_at": "2026-05-19T12:00:00Z",
  "producer": "fitness-tracker-api",
  "routing_key": "fitness.workout.created",
  "payload": {}
}
```

Поле `payload` зависит от типа события и описано в `event_catalog.md`.

## Гарантии доставки

Используется гарантия `at-least-once`:

- exchange объявлен как durable
- очередь объявлена как durable
- сообщения публикуются с `delivery_mode=Persistent`
- consumer подтверждает обработку через `basic_ack`
- при ошибке обработки consumer вызывает `basic_nack`

`At-least-once` - сообщение не должно потеряться, но в редких случаях может быть обработано повторно. Поэтому реальные consumer'ы должны быть идемпотентными и проверять `event_id`.

## Поток событий

Типовой поток:

1. Пользователь вызывает команду
2. API валидирует запрос и сохраняет изменения в PostgreSQL
3. Producer публикует событие `WorkoutCreated` в RabbitMQ
4. RabbitMQ маршрутизирует событие по routing key `fitness.workout.created`
5. Consumer получает событие, обрабатывает его и отправляет `ack`
6. Read-side компоненты могут обновить свои модели данных

Шаг публикации выполняет `events/producer.py`, обработку выполняет `events/consumer.py`.

## CQRS

CQRS можно применить в фитнес-трекере, потому что write-операции и read-операции имеют разную нагрузку и разные модели данных.

Команды:

- `POST /v1/auth/register`
- `POST /v1/exercises`
- `POST /v1/workouts`
- `POST /v1/workouts/{workout_id}/exercises`

Запросы:

- `GET /v1/users/by-login`
- `GET /v1/users/search`
- `GET /v1/exercises`
- `GET /v1/workouts/history`
- `GET /v1/workouts/statistics`

Write model хранится в нормализованной PostgreSQL-схеме: `users`, `exercises`, `workouts`, `workout_exercises`.

Read model может быть отдельной денормализованной моделью для быстрого чтения:

- список упражнений
- история тренировок пользователя
- агрегированная статистика по пользователю и периоду

События синхронизируют write и read модели:

- `ExerciseCreated` обновляет read-модель списка упражнений
- `WorkoutCreated` добавляет тренировку в read-модель истории
- `ExerciseAddedToWorkout` обновляет историю тренировки и агрегированную статистику
- `UserRegistered` может обновлять read-модель пользователей и audit-log

## Проверка работы

Запуск:

```bash
docker compose up --build
```

RabbitMQ UI:

```text
http://localhost:15672
```

Логин и пароль:

```text
guest / guest
```

Публикация демонстрационных событий:

```bash
docker compose run --rm event-producer
```

Просмотр логов consumer'а:

```bash
docker compose logs -f event-consumer
```

В логах должны появиться события `UserRegistered`, `ExerciseCreated`, `WorkoutCreated` и `ExerciseAddedToWorkout`.
