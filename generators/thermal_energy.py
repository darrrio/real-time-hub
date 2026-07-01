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
                "energy_kwh": round(random.uniform(0.0, 3.5), 2),
            }
            producer.send("sensors.thermal_energy", value=payload)
        producer.flush()
        time.sleep(interval)
