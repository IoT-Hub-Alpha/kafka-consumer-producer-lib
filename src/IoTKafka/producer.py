from __future__ import annotations

from typing import Any, Callable

from confluent_kafka import KafkaError, Message, Producer

from .kafka_configs import KafkaTopics
from .producer_singleton import KafkaProducerManager

DeliveryCallback = Callable[[KafkaError | None, Message], None]


class IoTProducerException(Exception):
    pass


class IoTKafkaProducer:
    def __init__(self, **kwargs: Any) -> None:
        self._producer: Producer = KafkaProducerManager.get_single_producer(**kwargs)

    def raw(self) -> Producer:
        return self._producer

    def produce(
        self,
        topic: str,
        key: bytes | str,
        value: bytes | str,
        on_delivery: DeliveryCallback | None = None,
        attempts: int = 3,
        poll_timeout: float = 0.5,
    ) -> None:
        if attempts < 1:
            raise ValueError("attempts must be >= 1")

        if isinstance(key, str):
            key = key.encode("utf-8")
        if isinstance(value, str):
            value = value.encode("utf-8")

        if topic not in KafkaTopics.as_set():
            raise IoTProducerException(f"{topic!r} is not a valid topic")

        last_error: BufferError | None = None
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
            except BufferError as exc:
                last_error = exc
                if attempt == attempts - 1:
                    break
                self._producer.poll(poll_timeout)

        raise IoTProducerException(f"Kafka buffer full after {attempts} attempt(s): {last_error}")

    def flush(self, timeout: float | None = None) -> int:
        return self._producer.flush(timeout)

    def __getattr__(self, name: str) -> Any:
        producer = object.__getattribute__(self, "_producer")
        return getattr(producer, name)
