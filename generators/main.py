import threading

from . import (
    comfort_setpoints,
    flow_temperature,
    indoor_temperature,
    outdoor_temperature,
    schedule_hours,
    thermal_energy,
    weather_forecast,
)

MODULES = [
    indoor_temperature,
    outdoor_temperature,
    thermal_energy,
    flow_temperature,
    weather_forecast,
    comfort_setpoints,
    schedule_hours,
]


def main():
    threads = [threading.Thread(target=m.run, daemon=True) for m in MODULES]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
