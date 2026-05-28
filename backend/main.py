"""
FastAPI backend — WebSocket ingestion endpoint.

Receives 1-second EEG windows from the device simulator, logs each packet
to console, and exposes a /health endpoint.
Redis Streams integration is added in Day 3.
"""
import json
import logging
from datetime import datetime, timezone

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [BACKEND] %(levelname)s %(message)s",
)
logger = logging.getLogger("backend")

app = FastAPI(title="EEG Stream Backend")

# ── Shared state (in-memory, single process) ───────────────────────────────────
_state: dict = {
    "last_received_timestamp": None,
    "total_packets_received" : 0,
}


# ── WebSocket endpoint ─────────────────────────────────────────────────────────

@app.websocket("/stream")
async def stream(websocket: WebSocket) -> None:
    await websocket.accept()
    logger.info("Device simulator connected")

    try:
        while True:
            raw          = await websocket.receive_text()
            msg          = json.loads(raw)

            ts           = msg.get("timestamp")
            channel_data = msg.get("channel_data", [])
            quality_flag = msg.get("quality_flag", "unknown")
            shape        = (
                len(channel_data),
                len(channel_data[0]) if channel_data else 0,
            )

            _state["last_received_timestamp"] = ts
            _state["total_packets_received"] += 1

            # Log artifacts at WARNING level so they stand out
            level = logging.WARNING if quality_flag == "artifact" else logging.INFO
            logger.log(
                level,
                f"pkt={_state['total_packets_received']:>4} | "
                f"shape={shape} | "
                f"quality={quality_flag} | "
                f"ts={ts}",
            )

    except WebSocketDisconnect:
        logger.info("Device simulator disconnected cleanly")
    except Exception as exc:
        logger.error(f"Connection error: {exc}")


# ── Health endpoint ────────────────────────────────────────────────────────────

@app.get("/health")
async def health() -> dict:
    return {
        "status"                 : "ok",
        "last_received_timestamp": _state["last_received_timestamp"],
        "total_packets_received" : _state["total_packets_received"],
    }
