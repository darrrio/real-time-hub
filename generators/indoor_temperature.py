import random
import time

from .common import emit_interval_seconds, make_producer, now_iso

ZONES = ["room_1", "room_2", "room_3"]


def run():
    producer = make_producer()
    interval = emit_interval_seconds()
    while True:
        for zone_id in ZONES:
            payload = {
                "zone_id": zone_id,
                "timestamp": now_iso(),
                "temperature_c": round(random.uniform(19.5, 24.5), 1),
            }
            producer.send("sensors.indoor_temperature", value=payload)
        producer.flush()
        time.sleep(interval)
