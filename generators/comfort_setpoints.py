import time

from .common import emit_interval_seconds, make_producer, now_iso

ZONES = ["living_room", "bedroom", "kitchen"]
SETPOINT_C = {"living_room": 21.0, "bedroom": 19.0, "kitchen": 20.0}


def run():
    producer = make_producer()
    interval = emit_interval_seconds()
    while True:
        for zone_id in ZONES:
            payload = {
                "zone_id": zone_id,
                "timestamp": now_iso(),
                "setpoint_c": SETPOINT_C[zone_id],
            }
            producer.send("comfort.setpoints", value=payload)
        producer.flush()
        time.sleep(interval)
