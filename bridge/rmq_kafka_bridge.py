# RabbitMQ -> Kafka bridge.
#
# Adapted from the driver_trackables_flink reference bridge, fixing its two
# known issues (see CLAUDE.md "Fixed gaps"):
#   - acks normally on a successful Kafka publish instead of unconditionally
#     NACK+requeueing every message ("peek, don't consume")
#   - sets a Kafka message key derived from the payload's natural identifier
#     (group_id:week_number) instead of publishing unkeyed
#
# Parsing here is best-effort and only used to derive the Kafka key — full
# payload validation (and DLQ routing on failure) happens downstream in the
# Flink job, which is the single source of truth for "did this message parse."
import json
import logging
import os
import time

import pika
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("rmq_kafka_bridge")

RMQ_HOST = os.environ.get("RMQ_HOST", "rabbitmq")
RMQ_PORT = int(os.environ.get("RMQ_PORT", "5672"))
RMQ_USER = os.environ.get("RMQ_USER", "guest")
RMQ_PASSWORD = os.environ.get("RMQ_PASSWORD", "guest")
RMQ_VHOST = os.environ.get("RMQ_VHOST", "/")
RMQ_QUEUE = os.environ.get("RMQ_QUEUE", "session_submitted")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "session_submitted")

RMQ_CONNECT_RETRY_SECONDS = 5
KAFKA_CONNECT_RETRY_SECONDS = 5


def message_key(body: str):
    try:
        data = json.loads(body)
    except (json.JSONDecodeError, TypeError):
        return None
    group_id = data.get("group_id")
    week_number = data.get("week_number")
    if group_id is None or week_number is None:
        return None
    return f"{group_id}:{week_number}"


def build_kafka_producer():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                acks="all",
                retries=3,
                max_in_flight_requests_per_connection=1,
                key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
                value_serializer=lambda v: v.encode("utf-8"),
            )
        except KafkaError as exc:
            logger.warning("kafka producer unavailable (%s), retrying in %ss", exc, KAFKA_CONNECT_RETRY_SECONDS)
            time.sleep(KAFKA_CONNECT_RETRY_SECONDS)


def build_rmq_connection():
    credentials = pika.PlainCredentials(RMQ_USER, RMQ_PASSWORD)
    parameters = pika.ConnectionParameters(
        host=RMQ_HOST,
        port=RMQ_PORT,
        virtual_host=RMQ_VHOST,
        credentials=credentials,
    )
    while True:
        try:
            return pika.BlockingConnection(parameters)
        except pika.exceptions.AMQPConnectionError as exc:
            logger.warning("rabbitmq unavailable (%s), retrying in %ss", exc, RMQ_CONNECT_RETRY_SECONDS)
            time.sleep(RMQ_CONNECT_RETRY_SECONDS)


def main():
    kafka_producer = build_kafka_producer()
    connection = build_rmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue=RMQ_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=10)

    def on_message(ch, method, properties, body):
        raw = body.decode("utf-8")
        key = message_key(raw)
        if key is None:
            logger.warning("could not derive a kafka key from message, publishing unkeyed")
        try:
            future = kafka_producer.send(KAFKA_TOPIC, key=key, value=raw)
            future.get(timeout=10)
        except KafkaError as exc:
            logger.error("publish to kafka failed (%s), nacking for redelivery", exc)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            return
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info("relayed message key=%s to topic=%s", key, KAFKA_TOPIC)

    channel.basic_consume(queue=RMQ_QUEUE, on_message_callback=on_message)
    logger.info("bridge started, consuming queue=%s -> topic=%s", RMQ_QUEUE, KAFKA_TOPIC)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()
        kafka_producer.close()


if __name__ == "__main__":
    main()
