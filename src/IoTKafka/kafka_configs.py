from __future__ import annotations
from typing import Any
from dataclasses import dataclass
import os


@dataclass(frozen=True)
class KafkaSettings:
    auto_offset: bool = False
    auto_commit: bool = False
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "None")
    client_id: str = str(os.getenv("KAFKA_CLIENT_ID", "none"))
    security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL")
    acks: str = os.getenv("KAFKA_PRODUCER_ACKS", "None")
    linger_ms: int = int(os.getenv("KAFKA_PRODUCER_LINGER_MS", "5"))
    batch_size: int = int(os.getenv("KAFKA_PRODUCER_BATCH_SIZE", "1"))
    compression_type: str = os.getenv("KAFKA_PRODUCER_COMPRESSION_TYPE", "none")
    request_timeout_ms: int = int(os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "20"))
    group_id: str | None = None
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    enable_auto_offset: bool = True

    sasl_mechanism: str | None = os.getenv("KAFKA_SASL_MECHANISM", None)
    sasl_username: str | None = os.getenv("KAFKA_SASL_USERNAME", None)
    sasl_password: str | None = os.getenv("KAFKA_SASL_PASSWORD", None)


@dataclass(frozen=True)
class KafkaTopics:
    telemetry_raw = os.getenv("KAFKA_TOPIC_TELEMETRY_RAW", "telemetry.raw")
    telemetry_clean = os.getenv("KAFKA_TOPIC_TELEMETRY_CLEAN", "telemetry.clean")
    telemetry_dlq = os.getenv("KAFKA_TOPIC_TELEMETRY_DLQ", "telemetry.dlq")
    event_topic = os.getenv("KAFKA_TOPIC_EVENT", "topic.event")

    @classmethod
    def values(cls):
        return [
            value
            for key, value in vars(cls).items()
            if not key.startswith("_") and not callable(value)
        ]


def build_producer_config(settings: KafkaSettings) -> dict[str, Any]:
    return {
        "bootstrap.servers": settings.bootstrap_servers,
        "client.id": settings.client_id,
        "security.protocol": settings.security_protocol,
        "acks": settings.acks,
        "linger.ms": settings.linger_ms,
        "batch.size": settings.batch_size,
        "compression.type": settings.compression_type,
        "request.timeout.ms": settings.request_timeout_ms,
    }


def build_consumer_config(settings: KafkaSettings) -> dict[str, Any]:
    if not settings.group_id:
        raise ValueError("group_id is required for consumer config")
    config: dict[str, Any] = {
        "bootstrap.servers": settings.bootstrap_servers,
        "client.id": (f"{settings.client_id}-{settings.group_id}-{os.getpid()}"),
        "group.id": settings.group_id,
        "security.protocol": settings.security_protocol,
        "enable.auto.commit": settings.auto_commit,
        "auto.offset.reset": "earliest",
        "enable.auto.offset.store": settings.auto_offset,
        "max.poll.interval.ms": 15 * 60 * 1000,
    }
    if settings.sasl_mechanism:
        config["sasl.mechanism"] = settings.sasl_mechanism
    if settings.sasl_username:
        config["sasl.username"] = settings.sasl_username
    if settings.sasl_password:
        config["sasl.password"] = settings.sasl_password
    return config
