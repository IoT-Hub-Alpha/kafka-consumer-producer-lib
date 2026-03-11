from .producer_singleton import KafkaProducerManager
from typing import Any

try:
    from confluent_kafka import KafkaError, Producer  # type: ignore
except ImportError:
    Producer, KafkaError = None, None


class KafKaProducer:
    def __init__(self):
        pass

    def get_producer(self, *_, **kwargs: dict[str, Any]) -> Producer:
        producer = KafkaProducerManager.get_single_producer(**kwargs)  # type: ignore

        return producer
