import random
import time

from .common import emit_interval_seconds, make_producer, now_iso

ZONES = ["living_room", "bedroom", "kitchen"]


def run():
    producer = make_producer()
    interval = emit_interval_seconds()
    while True:
        for zone_id in ZONES:
            payload = {
                "zone_id": zone_id,
                "timestamp": now_iso(),
                "temperature_c": round(random.uniform(18.0, 24.0), 1),
            }
            producer.send("sensors.indoor_temperature", value=payload)
        producer.flush()
        time.sleep(interval)
