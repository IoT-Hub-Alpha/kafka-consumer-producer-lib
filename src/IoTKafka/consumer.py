from confluent_kafka import Consumer, KafkaError
from .kafka_configs import build_consumer_config, KafkaSettings


class IoTKafkaConsumer:
    def __init__(self, **kwargs):
        self._consumer = self._get_consumer(**kwargs)

    def _get_consumer(self, **kwargs):
        settings = KafkaSettings(**kwargs)
        config = build_consumer_config(settings)
        return Consumer(config)
        

    def __getattr__(self, name):
        return getattr(self._consumer, name)
