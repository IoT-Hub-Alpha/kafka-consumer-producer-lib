from __future__ import annotations
from typing import Any
from dataclasses import dataclass
import os


@dataclass(frozen=True)
class KafkaSettings:
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

    pass
