import asyncio
import json
import os
import re
import threading
import time
from collections import deque
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

TOPIC_PATTERN = re.compile(
    r"^(sensors\.indoor_temperature|sensors\.outdoor_temperature|"
    r"comfort\.setpoints|comfort\.actions|comfort\.suggestions)$"
)
HISTORY_LEN = 60

app = FastAPI()
static_dir = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=static_dir), name="static")

state_lock = threading.Lock()
zones: dict[str, dict] = {}
outdoor: dict = {}

connected_sockets: set[WebSocket] = set()
broadcast_queue: asyncio.Queue = asyncio.Queue()
main_loop: asyncio.AbstractEventLoop | None = None


def zone_bucket(zone_id: str) -> dict:
    return zones.setdefault(
        zone_id,
        {
            "zone_id": zone_id,
            "temperature_c": None,
            "setpoint_c": None,
            "comfort_delta": None,
            "action": None,
            "suggestion": None,
            "updated_at": None,
            "history": deque(maxlen=HISTORY_LEN),
        },
    )


def apply_message(topic: str, payload: dict) -> dict:
    with state_lock:
        if topic == "sensors.outdoor_temperature":
            outdoor["temperature_c"] = payload.get("temperature_c")
            outdoor["updated_at"] = payload.get("timestamp")
            snapshot = {"type": "outdoor", "data": dict(outdoor)}
        else:
            zone_id = payload.get("zone_id", "unknown")
            bucket = zone_bucket(zone_id)
            bucket["updated_at"] = payload.get("timestamp")

            if topic == "sensors.indoor_temperature":
                bucket["temperature_c"] = payload.get("temperature_c")
                bucket["history"].append(
                    {
                        "t": payload.get("timestamp"),
                        "temperature_c": bucket["temperature_c"],
                        "setpoint_c": bucket["setpoint_c"],
                    }
                )
            elif topic == "comfort.setpoints":
                bucket["setpoint_c"] = payload.get("setpoint_c")
            elif topic == "comfort.actions":
                bucket["temperature_c"] = payload.get("temperature_c", bucket["temperature_c"])
                bucket["setpoint_c"] = payload.get("setpoint_c", bucket["setpoint_c"])
                bucket["comfort_delta"] = payload.get("comfort_delta")
                bucket["action"] = payload.get("action")
            elif topic == "comfort.suggestions":
                bucket["suggestion"] = payload.get("suggestion")

            snapshot = {
                "type": "zone",
                "data": {**bucket, "history": list(bucket["history"])},
            }
    return snapshot


def consume_loop():
    while True:
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=os.environ["REDPANDA_BOOTSTRAP_SERVERS"],
                security_protocol="PLAINTEXT",
                group_id="hvac-dashboard",
                auto_offset_reset="latest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            consumer.subscribe(pattern=TOPIC_PATTERN.pattern)

            for record in consumer:
                snapshot = apply_message(record.topic, record.value)
                if main_loop is not None:
                    main_loop.call_soon_threadsafe(broadcast_queue.put_nowait, snapshot)
        except NoBrokersAvailable:
            time.sleep(3)


async def broadcaster():
    while True:
        snapshot = await broadcast_queue.get()
        message = json.dumps(snapshot)
        dead = []
        for ws in connected_sockets:
            try:
                await ws.send_text(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            connected_sockets.discard(ws)


@app.on_event("startup")
async def startup():
    global main_loop
    main_loop = asyncio.get_running_loop()
    threading.Thread(target=consume_loop, daemon=True).start()
    asyncio.create_task(broadcaster())


@app.get("/")
async def index():
    return FileResponse(static_dir / "index.html")


@app.get("/api/state")
async def get_state():
    with state_lock:
        return {
            "outdoor": dict(outdoor),
            "zones": [
                {**z, "history": list(z["history"])} for z in zones.values()
            ],
        }


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_sockets.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connected_sockets.discard(websocket)
