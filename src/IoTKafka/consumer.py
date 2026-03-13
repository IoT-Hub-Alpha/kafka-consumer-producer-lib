from confluent_kafka import Consumer, KafkaError


class IoTKafkaConsumer:
    def __init__(self, **kwargs):
        self._consumer = self._build_consumer_config(**kwargs)

    def __getattr__(self, name):
        return getattr(self._consumer, name)
