import time
from datetime import datetime, timezone

from .common import emit_interval_seconds, make_producer, now_iso

ZONES = ["living_room", "bedroom", "kitchen"]
COMFORT_HOURS = range(6, 23)  # 06:00-22:59 is "comfort" mode, else "eco"


def run():
    producer = make_producer()
    interval = emit_interval_seconds()
    while True:
        hour = datetime.now(timezone.utc).hour
        mode = "comfort" if hour in COMFORT_HOURS else "eco"
        for zone_id in ZONES:
            payload = {
                "zone_id": zone_id,
                "timestamp": now_iso(),
                "mode": mode,
            }
            producer.send("schedule.hours", value=payload)
        producer.flush()
        time.sleep(interval)
