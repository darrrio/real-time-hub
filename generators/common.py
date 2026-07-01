import json
import os
from datetime import datetime, timezone

from kafka import KafkaProducer


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=os.environ["REDPANDA_BOOTSTRAP_SERVERS"],
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=os.environ["REDPANDA_SASL_USERNAME"],
        sasl_plain_password=os.environ["REDPANDA_SASL_PASSWORD"],
        value_serializer=lambda m: json.dumps(m).encode("utf-8"),
    )


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def emit_interval_seconds() -> float:
    return float(os.environ.get("EMIT_INTERVAL_SECONDS", "5"))
