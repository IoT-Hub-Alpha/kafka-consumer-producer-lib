from __future__ import annotations

from typing import Any

from confluent_kafka import Consumer

from .kafka_configs import KafkaSettings, KafkaTopics, build_consumer_config


class IoTKafkaConsumer:
    def __init__(self, **kwargs: Any) -> None:
        self._consumer = self._get_consumer(**kwargs)

    def _get_consumer(self, **kwargs: Any) -> Consumer:
        settings = KafkaSettings.from_kwargs(**kwargs)
        config = build_consumer_config(settings)
        return Consumer(config)

    def raw(self) -> Consumer:
        return self._consumer

    def subscribe_topics(self, topics: list[str]) -> None:
        invalid_topics = [topic for topic in topics if topic not in KafkaTopics.as_set()]
        if invalid_topics:
            raise ValueError(f"Invalid Kafka topic(s): {', '.join(invalid_topics)}")
        self._consumer.subscribe(topics)

    def close(self) -> None:
        self._consumer.close()

    def __getattr__(self, name: str) -> Any:
        consumer = object.__getattribute__(self, "_consumer")
        return getattr(consumer, name)
