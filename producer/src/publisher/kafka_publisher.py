"""
Publish normalized price ticks to Kafka topic price-ticks.
"""
import json
import logging
from typing import Any

from confluent_kafka import Producer as ConfluentProducer
from confluent_kafka import KafkaError

logger = logging.getLogger(__name__)


def create_producer(bootstrap_servers: str = "localhost:9092") -> ConfluentProducer:
    return ConfluentProducer({"bootstrap.servers": bootstrap_servers})


def publish_tick(producer: ConfluentProducer, topic: str, payload: dict[str, Any]) -> None:
    """Send one tick to Kafka; key by symbol for partitioning."""
    key = payload.get("symbol", "")
    value = json.dumps(payload).encode("utf-8")
    key_bytes = key.encode("utf-8") if key else None
    try:
        producer.produce(topic, value=value, key=key_bytes)
    except (KafkaError, BufferError) as e:
        logger.error("Failed to send: %s", e)
        raise
