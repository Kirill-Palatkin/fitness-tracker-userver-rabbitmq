import json
import os
import time

import pika


EXCHANGE = os.getenv("RABBITMQ_EXCHANGE", "fitness_tracker.events")
QUEUE = os.getenv("RABBITMQ_QUEUE", "fitness_tracker.audit")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")


def connect_with_retry():
    parameters = pika.URLParameters(RABBITMQ_URL)
    for attempt in range(1, 11):
        try:
            return pika.BlockingConnection(parameters)
        except pika.exceptions.AMQPConnectionError:
            print(f"RabbitMQ is not ready, retry {attempt}/10", flush=True)
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
    channel.queue_declare(queue=QUEUE, durable=True)
    channel.queue_bind(
        exchange=EXCHANGE,
        queue=QUEUE,
        routing_key="fitness.#",
    )
    channel.basic_qos(prefetch_count=1)

    def handle_message(ch, method, properties, body):
        try:
            event = json.loads(body.decode("utf-8"))
            print(
                "consumed "
                f"{event.get('event_type')} "
                f"routing_key={method.routing_key} "
                f"payload={json.dumps(event.get('payload'), ensure_ascii=False)}",
                flush=True,
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as exc:
            print(f"failed to process message: {exc}", flush=True)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    print(f"waiting for events from exchange={EXCHANGE}, queue={QUEUE}", flush=True)
    channel.basic_consume(queue=QUEUE, on_message_callback=handle_message)
    channel.start_consuming()


if __name__ == "__main__":
    main()
