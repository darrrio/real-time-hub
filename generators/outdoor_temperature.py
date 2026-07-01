import random
import time

from .common import emit_interval_seconds, make_producer, now_iso


def run():
    producer = make_producer()
    interval = emit_interval_seconds()
    while True:
        payload = {
            "zone_id": "outdoor",
            "timestamp": now_iso(),
            "temperature_c": round(random.uniform(-5.0, 30.0), 1),
        }
        producer.send("sensors.outdoor_temperature", value=payload)
        producer.flush()
        time.sleep(interval)
