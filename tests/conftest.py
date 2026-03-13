import sys
import types
from pathlib import Path


class FakeKafkaError(Exception):
    pass


class FakeMessage:
    pass


class FakeProducer:
    created_configs = []

    def __init__(self, config):
        self.config = config
        self.produced = []
        self.poll_calls = []
        self.flush_calls = []
        self.raise_buffer_errors = 0
        self.custom_attr = "producer-attr"
        type(self).created_configs.append(config)

    def produce(self, **kwargs):
        if self.raise_buffer_errors > 0:
            self.raise_buffer_errors -= 1
            raise BufferError("queue full")
        self.produced.append(kwargs)

    def poll(self, timeout):
        self.poll_calls.append(timeout)

    def flush(self, timeout=None):
        self.flush_calls.append(timeout)
        return 0


class FakeConsumer:
    created_configs = []

    def __init__(self, config):
        self.config = config
        self.subscribed = []
        self.closed = False
        self.custom_attr = "consumer-attr"
        type(self).created_configs.append(config)

    def subscribe(self, topics):
        self.subscribed.append(topics)

    def close(self):
        self.closed = True


def pytest_configure():
    fake_module = types.ModuleType("confluent_kafka")
    fake_module.Producer = FakeProducer
    fake_module.Consumer = FakeConsumer
    fake_module.KafkaError = FakeKafkaError
    fake_module.Message = FakeMessage
    sys.modules["confluent_kafka"] = fake_module

    root = str(Path("/mnt/data"))
    if root not in sys.path:
        sys.path.insert(0, root)
