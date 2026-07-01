import random
import time

from .common import emit_interval_seconds, make_producer, now_iso

UNITS = ["heater_1", "heater_2"]


def run():
    producer = make_producer()
    interval = emit_interval_seconds()
    while True:
        for unit_id in UNITS:
            payload = {
                "unit_id": unit_id,
                "timestamp": now_iso(),
                "flow_temperature_c": round(random.uniform(35.0, 60.0), 1),
            }
            producer.send("sensors.flow_temperature", value=payload)
        producer.flush()
        time.sleep(interval)
