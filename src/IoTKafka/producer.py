from .producer_singleton import KafkaProducerManager
from typing import Any

from confluent_kafka import KafkaError, Producer  # type: ignore


class IoTProducerException(Exception):
    pass


class KafKaProducer:
    def __init__(self, *_, **kwargs: dict[str, Any]):
        self._producer = KafkaProducerManager.get_single_producer(**kwargs)  # type: ignore

    def get_producer(self) -> Producer:
        return self._producer

    def produce(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        on_delivery: function,
        attempts: int = 3,
    ):
        for attempt in range(attempts):
            try:
                self._producer.produce(
                    topic=topic,
                    key=key,
                    value=value,
                    on_delivery=on_delivery,
                )
                self._producer.poll(0)
                break
            except BufferError as e:
                if attempt >= attempts:
                    raise IoTProducerException(f"Kafka buffer full {e}")
                self._producer.poll(0.5)
