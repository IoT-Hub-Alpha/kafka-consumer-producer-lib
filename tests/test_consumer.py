import pytest

from IoTKafka.consumer import IoTKafkaConsumer
from IoTKafka.kafka_configs import KafkaTopics


def test_consumer_builds_underlying_consumer_with_config():
    wrapper = IoTKafkaConsumer(
        bootstrap_servers="kafka:9092",
        client_id="db-writer",
        group_id="writers",
    )
    assert wrapper.raw() is wrapper._consumer
    assert wrapper._consumer.config["bootstrap.servers"] == "kafka:9092"
    assert wrapper._consumer.config["group.id"] == "writers"


def test_subscribe_topics_accepts_valid_topics_and_delegates():
    wrapper = IoTKafkaConsumer(
        bootstrap_servers="kafka:9092",
        client_id="db-writer",
        group_id="writers",
    )
    wrapper.subscribe_topics([KafkaTopics.telemetry_raw, KafkaTopics.telemetry_clean])
    assert wrapper._consumer.subscribed == [
        [KafkaTopics.telemetry_raw, KafkaTopics.telemetry_clean]
    ]


def test_subscribe_topics_rejects_invalid_topics():
    wrapper = IoTKafkaConsumer(
        bootstrap_servers="kafka:9092",
        client_id="db-writer",
        group_id="writers",
    )
    with pytest.raises(ValueError, match=r"Invalid Kafka topic\(s\): bad.topic"):
        wrapper.subscribe_topics([KafkaTopics.telemetry_raw, "bad.topic"])


def test_close_and_getattr_delegate_to_underlying_consumer():
    wrapper = IoTKafkaConsumer(
        bootstrap_servers="kafka:9092",
        client_id="db-writer",
        group_id="writers",
    )
    wrapper.close()
    assert wrapper._consumer.closed is True
    assert wrapper.custom_attr == "consumer-attr"


def test_consumer_init_rejects_unknown_kwargs():
    with pytest.raises(
        ValueError, match="Unknown KafkaSettings fields: strange_option"
    ):
        IoTKafkaConsumer(
            bootstrap_servers="kafka:9092",
            client_id="db-writer",
            group_id="writers",
            strange_option=True,
        )
