import os

import pytest

from IoTKafka.kafka_configs import (
    KafkaSettings,
    KafkaTopics,
    _apply_common_security,
    build_consumer_config,
    build_producer_config,
)


def test_kafka_settings_from_kwargs_accepts_known_fields():
    settings = KafkaSettings.from_kwargs(
        bootstrap_servers='kafka:9092',
        client_id='svc-a',
        enable_auto_commit=True,
    )
    assert settings.bootstrap_servers == 'kafka:9092'
    assert settings.client_id == 'svc-a'
    assert settings.enable_auto_commit is True


def test_kafka_settings_from_kwargs_rejects_unknown_fields():
    with pytest.raises(ValueError, match='Unknown KafkaSettings fields: unknown_option'):
        KafkaSettings.from_kwargs(unknown_option='x')


def test_kafka_topics_values_and_set_only_return_strings(monkeypatch):
    monkeypatch.setattr(KafkaTopics, 'helper', classmethod(lambda cls: ('x',)), raising=False)
    values = KafkaTopics.values()
    assert isinstance(values, tuple)
    assert KafkaTopics.telemetry_raw in values
    assert KafkaTopics.event_topic in KafkaTopics.as_set()
    assert all(isinstance(item, str) for item in values)


def test_apply_common_security_adds_optional_sasl_fields():
    settings = KafkaSettings(
        security_protocol='SASL_SSL',
        sasl_mechanism='PLAIN',
        sasl_username='user',
        sasl_password='pass',
    )
    config = {}
    _apply_common_security(config, settings)
    assert config == {
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'user',
        'sasl.password': 'pass',
    }


def test_build_producer_config_contains_expected_keys():
    settings = KafkaSettings(
        bootstrap_servers='broker:9092',
        client_id='producer-1',
        acks='all',
        linger_ms=12,
        batch_size=4096,
        compression_type='gzip',
        request_timeout_ms=999,
        security_protocol='PLAINTEXT',
    )
    config = build_producer_config(settings)
    assert config['bootstrap.servers'] == 'broker:9092'
    assert config['client.id'] == 'producer-1'
    assert config['acks'] == 'all'
    assert config['linger.ms'] == 12
    assert config['batch.size'] == 4096
    assert config['compression.type'] == 'gzip'
    assert config['request.timeout.ms'] == 999
    assert config['security.protocol'] == 'PLAINTEXT'


def test_build_consumer_config_requires_group_id():
    with pytest.raises(ValueError, match='group_id is required'):
        build_consumer_config(KafkaSettings())


def test_build_consumer_config_uses_pid_and_consumer_flags(monkeypatch):
    monkeypatch.setattr(os, 'getpid', lambda: 12345)
    settings = KafkaSettings(
        bootstrap_servers='broker:9092',
        client_id='consumer-1',
        group_id='db-writers',
        enable_auto_commit=True,
        enable_auto_offset_store=True,
        auto_offset_reset='latest',
        max_poll_interval_ms=42,
        security_protocol='SASL_SSL',
        sasl_mechanism='PLAIN',
        sasl_username='user',
        sasl_password='pass',
    )
    config = build_consumer_config(settings)
    assert config['bootstrap.servers'] == 'broker:9092'
    assert config['client.id'] == 'consumer-1-db-writers-12345'
    assert config['group.id'] == 'db-writers'
    assert config['enable.auto.commit'] is True
    assert config['enable.auto.offset.store'] is True
    assert config['auto.offset.reset'] == 'latest'
    assert config['max.poll.interval.ms'] == 42
    assert config['security.protocol'] == 'SASL_SSL'
    assert config['sasl.mechanism'] == 'PLAIN'
