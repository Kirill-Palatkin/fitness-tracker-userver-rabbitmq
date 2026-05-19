# Каталог событий

Все события публикуются в RabbitMQ exchange `fitness_tracker.events` типа `topic`.

Общие поля каждого события:

- `event_id` - уникальный идентификатор события
- `event_type` - название события
- `occurred_at` - время возникновения события в UTC
- `producer` - producer события
- `routing_key` - ключ маршрутизации RabbitMQ
- `payload` - полезная нагрузка события

Гарантия доставки для всех событий: `at-least-once`.

## UserRegistered

Событие возникает после успешной регистрации пользователя.

Routing key:

```text
fitness.user.registered
```

Producer:

```text
fitness-tracker-api / fitness-tracker-event-producer
```

Consumers:

- `fitness-tracker-audit-consumer`
- `notification-service`
- `statistics-read-model-service`

Payload:

```json
{
  "user_id": 101,
  "login": "event_demo_user",
  "first_name": "Event",
  "last_name": "Demo"
}
```

## ExerciseCreated

Событие возникает после создания нового упражнения.

Routing key:

```text
fitness.exercise.created
```

Producer:

```text
fitness-tracker-api / fitness-tracker-event-producer
```

Consumers:

- `fitness-tracker-audit-consumer`
- `statistics-read-model-service`

Payload:

```json
{
  "exercise_id": 201,
  "name": "Event Squats",
  "muscle_group": "Legs",
  "calories_per_minute": 8,
  "created_by": 101
}
```

## WorkoutCreated

Событие возникает после создания тренировки пользователя.

Routing key:

```text
fitness.workout.created
```

Producer:

```text
fitness-tracker-api / fitness-tracker-event-producer
```

Consumers:

- `fitness-tracker-audit-consumer`
- `notification-service`
- `statistics-read-model-service`

Payload:

```json
{
  "workout_id": 301,
  "user_id": 101,
  "title": "Event Workout",
  "planned_date": "2026-05-19"
}
```

## ExerciseAddedToWorkout

Событие возникает после добавления упражнения в тренировку.

Routing key:

```text
fitness.workout.exercise_added
```

Producer:

```text
fitness-tracker-api / fitness-tracker-event-producer
```

Consumers:

- `fitness-tracker-audit-consumer`
- `statistics-read-model-service`

Payload:

```json
{
  "workout_id": 301,
  "exercise_id": 201,
  "sets": 4,
  "reps": 12,
  "duration_minutes": 20
}
```
