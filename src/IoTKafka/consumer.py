from confluent_kafka import Consumer, KafkaError
from .kafka_configs import build_consumer_config


class IoTKafkaConsumer:
    def __init__(self, **kwargs):
        self._consumer = build_consumer_config(**kwargs)

    def __getattr__(self, name):
        return getattr(self._consumer, name)
