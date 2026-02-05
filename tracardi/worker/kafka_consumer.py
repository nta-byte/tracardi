import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer
from tracardi.config import kafka_config
from tracardi.worker.worker import process_kafka_event
from tracardi.exceptions.log_handler import get_installation_logger

logger = get_installation_logger(__name__, logging.INFO)


async def run():
    if not kafka_config.run_consumer:
        logger.info("Kafka consumer disabled")
        return

    consumer = AIOKafkaConsumer(
        kafka_config.topic,
        bootstrap_servers=kafka_config.bootstrap_servers,
        group_id=None,
        auto_offset_reset=kafka_config.auto_offset_reset,
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode()),
    )

    await consumer.start()
    logger.info("Kafka consumer started")

    try:
        async for msg in consumer:
            process_kafka_event(msg.value)  # enqueue Huey task
    finally:
        await consumer.stop()


def main():
    asyncio.run(run())


if __name__ == "__main__":
    main()
