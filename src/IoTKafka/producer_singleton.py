from typing import Any
from .kafka_configs import KafkaSettings, build_producer_config

try:
    from confluent_kafka import Producer  # type: ignore
except ImportError:
    Producer = None


class KafkaProducerError(Exception):
    pass


class KafkaProducerManager:
    _instance: Any = None

    @classmethod
    def get_single_producer(cls, **kwargs: Any) -> Producer:
        if Producer is None:
            raise KafkaProducerError("confluent_kafka is not installed")

        if cls._instance is None:
            settings = KafkaSettings(**kwargs)
            config = build_producer_config(settings)
            cls._instance = Producer(config)

        return cls._instance
