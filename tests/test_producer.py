import pytest

from IoTKafka.kafka_configs import KafkaTopics
from IoTKafka.producer import IoTKafkaProducer, IoTProducerException
from IoTKafka.producer_singleton import KafkaProducerManager


def teardown_function():
    KafkaProducerManager.close_all()


def test_raw_returns_underlying_producer():
    wrapper = IoTKafkaProducer(bootstrap_servers="kafka:9092", client_id="svc-a")
    assert wrapper.raw() is wrapper._producer


def test_produce_encodes_string_key_and_value_and_polls_zero():
    wrapper = IoTKafkaProducer(bootstrap_servers="kafka:9092", client_id="svc-a")
    callback = lambda err, msg: None
    wrapper.produce(
        KafkaTopics.telemetry_raw, "device-1", '{"temp":1}', on_delivery=callback
    )
    produced = wrapper._producer.produced[0]
    assert produced["topic"] == KafkaTopics.telemetry_raw
    assert produced["key"] == b"device-1"
    assert produced["value"] == b'{"temp":1}'
    assert produced["on_delivery"] is callback
    assert wrapper._producer.poll_calls == [0]


def test_produce_rejects_invalid_topic():
    wrapper = IoTKafkaProducer(bootstrap_servers="kafka:9092", client_id="svc-a")
    with pytest.raises(IoTProducerException, match="not a valid topic"):
        wrapper.produce("wrong.topic", "k", b"v")


def test_produce_retries_on_buffer_error_then_succeeds():
    wrapper = IoTKafkaProducer(bootstrap_servers="kafka:9092", client_id="svc-a")
    wrapper._producer.raise_buffer_errors = 1
    wrapper.produce(
        KafkaTopics.telemetry_raw, b"k", b"v", attempts=3, poll_timeout=0.25
    )
    assert wrapper._producer.poll_calls == [0.25, 0]
    assert len(wrapper._producer.produced) == 1


def test_produce_raises_after_exhausting_attempts():
    wrapper = IoTKafkaProducer(bootstrap_servers="kafka:9092", client_id="svc-a")
    wrapper._producer.raise_buffer_errors = 3
    with pytest.raises(IoTProducerException, match="Kafka buffer full after 3 attempt"):
        wrapper.produce(KafkaTopics.telemetry_raw, b"k", b"v", attempts=3)
    assert wrapper._producer.poll_calls == [0.5, 0.5]


def test_produce_rejects_attempts_below_one():
    wrapper = IoTKafkaProducer(bootstrap_servers="kafka:9092", client_id="svc-a")
    with pytest.raises(ValueError, match="attempts must be >= 1"):
        wrapper.produce(KafkaTopics.telemetry_raw, b"k", b"v", attempts=0)


def test_flush_and_getattr_delegate_to_underlying_producer():
    wrapper = IoTKafkaProducer(bootstrap_servers="kafka:9092", client_id="svc-a")
    assert wrapper.flush(2.5) == 0
    assert wrapper._producer.flush_calls == [2.5]
    assert wrapper.custom_attr == "producer-attr"
