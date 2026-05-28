"""
Synthetic EEG device simulator.

Generates 8-channel EEG at 256 Hz (alpha 10 Hz + beta 20 Hz + Gaussian noise),
injects random spike artifacts (~1 every 5 seconds), and streams 1-second windows
over WebSocket as JSON. Reconnects with exponential backoff on disconnect.
"""
import asyncio
import json
import logging
import math
import random
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

# ── Paths ──────────────────────────────────────────────────────────────────────
ROOT    = Path(__file__).resolve().parent.parent
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SIM] %(levelname)s %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("simulator")


def _log_dropped(window_index: int, error: str) -> None:
    """Append one line to logs/dropped_packets.log and echo to console."""
    ts    = datetime.now(timezone.utc).isoformat()
    entry = f"{ts} | window={window_index} | {error}\n"
    with open(LOG_DIR / "dropped_packets.log", "a") as fh:
        fh.write(entry)
    logger.warning(f"DROPPED — {entry.strip()}")


# ── EEG signal parameters ──────────────────────────────────────────────────────
N_CHANNELS       = 8
SAMPLE_RATE      = 256      # Hz
SAMPLES_PER_WIN  = SAMPLE_RATE  # 1-second window → 256 samples
ALPHA_HZ         = 10.0
BETA_HZ          = 20.0
NOISE_STD        = 0.5
SPIKE_PROB       = 0.20     # ~1 spike every 5 seconds (Poisson-like)
SPIKE_MULTIPLIER = 5.0      # spike magnitude = 5 × channel std dev

# ── WebSocket parameters ───────────────────────────────────────────────────────
WS_URL       = "ws://localhost:8000/stream"
MAX_RETRIES  = 5
BASE_BACKOFF = 1.0          # seconds; doubles on each retry


# ── Signal generation ──────────────────────────────────────────────────────────

def generate_window(window_index: int) -> tuple[list, str]:
    """
    Generate one 1-second window of synthetic 8-channel EEG.

    Returns:
        channel_data : list[list[float]]  shape (256, 8)
        quality_flag : "clean" | "artifact"

    Each channel is alpha + beta sine waves with per-channel phase offset
    plus Gaussian noise. Spike artifacts are injected probabilistically.
    """
    # Time axis for this window (continuous across windows)
    t = np.linspace(window_index, window_index + 1.0, SAMPLES_PER_WIN, endpoint=False)

    data = np.zeros((SAMPLES_PER_WIN, N_CHANNELS), dtype=np.float64)
    for ch in range(N_CHANNELS):
        phase = ch * (2 * math.pi / N_CHANNELS)          # spread phases across channels
        alpha = np.sin(2 * math.pi * ALPHA_HZ * t + phase)
        beta  = 0.5 * np.sin(2 * math.pi * BETA_HZ * t + phase)
        noise = np.random.normal(0, NOISE_STD, SAMPLES_PER_WIN)
        data[:, ch] = alpha + beta + noise

    quality_flag = "clean"
    if random.random() < SPIKE_PROB:
        spike_ch     = random.randint(0, N_CHANNELS - 1)
        spike_sample = random.randint(0, SAMPLES_PER_WIN - 1)
        # Spike is 5× the channel's std dev — guaranteed to exceed 3-std threshold in Day 3
        spike_mag    = SPIKE_MULTIPLIER * float(np.std(data[:, spike_ch]))
        data[spike_sample, spike_ch] += spike_mag
        quality_flag = "artifact"
        logger.info(
            f"Spike injected — ch={spike_ch} sample={spike_sample} "
            f"mag={spike_mag:.3f}"
        )

    return data.tolist(), quality_flag


# ── Main streaming loop ────────────────────────────────────────────────────────

async def run() -> None:
    retry        = 0
    window_index = 0

    while retry <= MAX_RETRIES:
        try:
            logger.info(
                f"Connecting to {WS_URL}  "
                f"(attempt {retry + 1}/{MAX_RETRIES + 1})"
            )
            async with websockets.connect(WS_URL) as ws:
                logger.info("Connected — streaming EEG data")
                retry = 0  # reset counter on successful connection

                while True:
                    channel_data, quality_flag = generate_window(window_index)
                    payload = {
                        "timestamp"   : datetime.now(timezone.utc).isoformat(),
                        "channel_data": channel_data,
                        "quality_flag": quality_flag,
                    }
                    await ws.send(json.dumps(payload))
                    window_index += 1
                    await asyncio.sleep(1.0)  # real-time: 1 window per second

        except (ConnectionClosed, WebSocketException, OSError) as exc:
            retry += 1
            _log_dropped(window_index, str(exc))
            if retry > MAX_RETRIES:
                logger.error("Max retries reached. Stopping simulator.")
                break
            backoff = BASE_BACKOFF * (2 ** (retry - 1))
            logger.info(f"Reconnecting in {backoff:.1f}s…")
            await asyncio.sleep(backoff)


if __name__ == "__main__":
    asyncio.run(run())
