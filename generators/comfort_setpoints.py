import time

from .common import emit_interval_seconds, make_producer, now_iso

ZONES = ["room_1", "room_2", "room_3"]
SETPOINT_C = {"room_1": 22.0, "room_2": 20.0, "room_3": 24.0}


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
