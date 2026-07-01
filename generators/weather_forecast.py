import random
import time

from .common import emit_interval_seconds, make_producer, now_iso

CONDITIONS = ["clear", "cloudy", "rain", "snow"]


def run():
    producer = make_producer()
    interval = emit_interval_seconds()
    while True:
        payload = {
            "zone_id": "outdoor",
            "timestamp": now_iso(),
            "forecast_temperature_c": round(random.uniform(-5.0, 30.0), 1),
            "condition": random.choice(CONDITIONS),
            "wind_kph": round(random.uniform(0.0, 40.0), 1),
        }
        producer.send("weather.forecast", value=payload)
        producer.flush()
        time.sleep(interval)
