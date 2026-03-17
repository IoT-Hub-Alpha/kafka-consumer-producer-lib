from __future__ import annotations

from threading import Lock
from typing import Any

from confluent_kafka import Producer

from .kafka_configs import KafkaSettings, build_producer_config


class KafkaProducerError(Exception):
    pass


class KafkaProducerManager:
    _instances: dict[tuple[tuple[str, Any], ...], Producer] = {}
    _lock = Lock()

    @classmethod
    def get_single_producer(cls, **kwargs: Any) -> Producer:
        settings = KafkaSettings.from_kwargs(**kwargs)
        config = build_producer_config(settings)
        key = tuple(sorted(config.items()))

        with cls._lock:
            producer = cls._instances.get(key)
            if producer is None:
                producer = Producer(config)
                cls._instances[key] = producer
            return producer

    @classmethod
    def close_all(cls) -> None:
        with cls._lock:
            for producer in cls._instances.values():
                producer.flush()
            cls._instances.clear()
