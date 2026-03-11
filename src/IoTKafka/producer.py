from typing import Any, Callable, Optional
from confluent_kafka import Producer, KafkaError, Message

from .producer_singleton import KafkaProducerManager


class IoTProducerException(Exception):
    pass


class IoTProducer:
    def __init__(self, **kwargs: dict[str, Any]):
        self._producer: Producer = KafkaProducerManager.get_single_producer(**kwargs)

    # expose real producer if needed
    def raw(self) -> Producer:
        return self._producer

    # custom produce with retry
    def produce(
        self,
        topic: str,
        key: bytes | str,
        value: bytes,
        on_delivery: Callable[[Optional[KafkaError], Message], None],
        attempts: int = 3,
    ) -> None:

        if isinstance(key, str):
            key = key.encode("utf-8")

        for attempt in range(attempts):
            try:
                self._producer.produce(
                    topic=topic,
                    key=key,
                    value=value,
                    on_delivery=on_delivery,
                )
                self._producer.poll(0)
                return

            except BufferError as e:
                if attempt == attempts - 1:
                    raise IoTProducerException(f"Kafka buffer full: {e}")

                self._producer.poll(0.5)

    def __getattr__(self, name):
        return getattr(self._producer, name)