"""
Producer: Binance WebSocket -> Kafka (topic price-ticks).
Configure symbols via env SYMBOLS (default: btcusdt,ethusdt) and Kafka via KAFKA_BOOTSTRAP (default: localhost:9092).
"""
import os
import signal
import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Add src dir so we can import fetcher and publisher
sys.path.insert(0, os.path.dirname(__file__))

from fetcher.binance_ws import stream_ticks
from publisher.kafka_publisher import create_producer, publish_tick


def main() -> None:
    symbols_str = os.environ.get("SYMBOLS", "btcusdt,ethusdt")
    symbols = [s.strip().lower() for s in symbols_str.split(",") if s.strip()]
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    topic = os.environ.get("KAFKA_TOPIC", "price-ticks")

    logger.info("Symbols: %s, Kafka: %s, topic: %s", symbols, bootstrap, topic)

    producer = create_producer(bootstrap_servers=bootstrap)
    closed = False

    def shutdown(*_args: object) -> None:
        nonlocal closed
        closed = True

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        for tick in stream_ticks(symbols):
            if closed:
                break
            publish_tick(producer, topic, tick)
            producer.flush()
    except Exception as e:
        logger.exception("Stream error: %s", e)
    finally:
        # confluent_kafka Producer has no close(); flush to send pending messages
        producer.flush()
    logger.info("Producer stopped.")


if __name__ == "__main__":
    main()
