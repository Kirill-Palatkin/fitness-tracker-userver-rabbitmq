import json
import os
import time
import uuid
from datetime import datetime, timezone

import pika


EXCHANGE = os.getenv("RABBITMQ_EXCHANGE", "fitness_tracker.events")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def build_event(event_type, routing_key, payload):
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "occurred_at": utc_now(),
        "producer": "fitness-tracker-event-producer",
        "routing_key": routing_key,
        "payload": payload,
    }


def connect_with_retry():
    parameters = pika.URLParameters(RABBITMQ_URL)
    for attempt in range(1, 11):
        try:
            return pika.BlockingConnection(parameters)
        except pika.exceptions.AMQPConnectionError:
            print(f"RabbitMQ is not ready, retry {attempt}/10")
            time.sleep(2)
    return pika.BlockingConnection(parameters)


def main():
    connection = connect_with_retry()
    channel = connection.channel()
    channel.exchange_declare(
        exchange=EXCHANGE,
        exchange_type="topic",
        durable=True,
    )

    events = [
        build_event(
            "UserRegistered",
            "fitness.user.registered",
            {
                "user_id": 101,
                "login": "event_demo_user",
                "first_name": "Event",
                "last_name": "Demo",
            },
        ),
        build_event(
            "ExerciseCreated",
            "fitness.exercise.created",
            {
                "exercise_id": 201,
                "name": "Event Squats",
                "muscle_group": "Legs",
                "calories_per_minute": 8,
                "created_by": 101,
            },
        ),
        build_event(
            "WorkoutCreated",
            "fitness.workout.created",
            {
                "workout_id": 301,
                "user_id": 101,
                "title": "Event Workout",
                "planned_date": "2026-05-19",
            },
        ),
        build_event(
            "ExerciseAddedToWorkout",
            "fitness.workout.exercise_added",
            {
                "workout_id": 301,
                "exercise_id": 201,
                "sets": 4,
                "reps": 12,
                "duration_minutes": 20,
            },
        ),
    ]

    for event in events:
        channel.basic_publish(
            exchange=EXCHANGE,
            routing_key=event["routing_key"],
            body=json.dumps(event, ensure_ascii=False).encode("utf-8"),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=pika.DeliveryMode.Persistent,
            ),
        )
        print(f"published {event['event_type']} -> {event['routing_key']}")
        time.sleep(0.2)

    connection.close()


if __name__ == "__main__":
    main()
