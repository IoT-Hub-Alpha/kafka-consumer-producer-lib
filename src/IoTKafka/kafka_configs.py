from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any
import os


@dataclass(frozen=True, slots=True)
class KafkaSettings:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    client_id: str = os.getenv("KAFKA_CLIENT_ID", "iot-kafka-client")
    security_protocol: str = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")

    # Producer settings
    acks: str = os.getenv("KAFKA_PRODUCER_ACKS", "all")
    linger_ms: int = int(os.getenv("KAFKA_PRODUCER_LINGER_MS", "5"))
    batch_size: int = int(os.getenv("KAFKA_PRODUCER_BATCH_SIZE", "16384"))
    compression_type: str = os.getenv("KAFKA_PRODUCER_COMPRESSION_TYPE", "none")
    request_timeout_ms: int = int(os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "30000"))

    # Consumer settings
    group_id: str | None = None
    auto_offset_reset: str = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
    enable_auto_commit: bool = os.getenv("KAFKA_ENABLE_AUTO_COMMIT", "false").lower() == "true"
    enable_auto_offset_store: bool = os.getenv("KAFKA_ENABLE_AUTO_OFFSET_STORE", "false").lower() == "true"
    max_poll_interval_ms: int = int(os.getenv("KAFKA_MAX_POLL_INTERVAL_MS", str(15 * 60 * 1000)))

    # SASL settings
    sasl_mechanism: str | None = os.getenv("KAFKA_SASL_MECHANISM")
    sasl_username: str | None = os.getenv("KAFKA_SASL_USERNAME")
    sasl_password: str | None = os.getenv("KAFKA_SASL_PASSWORD")

    @classmethod
    def from_kwargs(cls, **kwargs: Any) -> "KafkaSettings":
        valid_fields = {field.name for field in fields(cls)}
        unknown = set(kwargs) - valid_fields
        if unknown:
            unknown_str = ", ".join(sorted(unknown))
            raise ValueError(f"Unknown KafkaSettings fields: {unknown_str}")
        return cls(**kwargs)


class KafkaTopics:
    telemetry_raw = os.getenv("KAFKA_TOPIC_TELEMETRY_RAW", "telemetry.raw")
    telemetry_clean = os.getenv("KAFKA_TOPIC_TELEMETRY_CLEAN", "telemetry.clean")
    telemetry_dlq = os.getenv("KAFKA_TOPIC_TELEMETRY_DLQ", "telemetry.dlq")
    event_topic = os.getenv("KAFKA_TOPIC_EVENT", "topic.event")

    @classmethod
    def values(cls) -> tuple[str, ...]:
        return tuple(
            value
            for key, value in vars(cls).items()
            if not key.startswith("_") and isinstance(value, str)
        )

    @classmethod
    def as_set(cls) -> set[str]:
        return set(cls.values())


def _apply_common_security(config: dict[str, Any], settings: KafkaSettings) -> None:
    if settings.security_protocol:
        config["security.protocol"] = settings.security_protocol

    if settings.sasl_mechanism:
        config["sasl.mechanism"] = settings.sasl_mechanism
    if settings.sasl_username:
        config["sasl.username"] = settings.sasl_username
    if settings.sasl_password:
        config["sasl.password"] = settings.sasl_password


def build_producer_config(settings: KafkaSettings) -> dict[str, Any]:
    config: dict[str, Any] = {
        "bootstrap.servers": settings.bootstrap_servers,
        "client.id": settings.client_id,
        "acks": settings.acks,
        "linger.ms": settings.linger_ms,
        "batch.size": settings.batch_size,
        "compression.type": settings.compression_type,
        "request.timeout.ms": settings.request_timeout_ms,
    }
    _apply_common_security(config, settings)
    return config


def build_consumer_config(settings: KafkaSettings) -> dict[str, Any]:
    if not settings.group_id:
        raise ValueError("group_id is required for consumer config")

    config: dict[str, Any] = {
        "bootstrap.servers": settings.bootstrap_servers,
        "client.id": f"{settings.client_id}-{settings.group_id}-{os.getpid()}",
        "group.id": settings.group_id,
        "enable.auto.commit": settings.enable_auto_commit,
        "auto.offset.reset": settings.auto_offset_reset,
        "enable.auto.offset.store": settings.enable_auto_offset_store,
        "max.poll.interval.ms": settings.max_poll_interval_ms,
    }
    _apply_common_security(config, settings)
    return config
