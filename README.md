# EEG Brain Data Streaming Pipeline

Real-time EEG stream ingestion → preprocessing → cloud model training → low-latency inference.

---

## Architecture

```
┌─────────────────┐     WebSocket      ┌──────────────────┐               ┌──────────────────┐
│  Device Sim     │ ─────────────────► │  FastAPI Backend  │               │  Preprocessing   │
│  (MNE-Python)   │   ws://…/stream    │  /stream endpoint │ ────────────► │  window • norm   │
│  8-ch, 256 Hz   │                    │                   │               │  artifact reject │
└─────────────────┘                    └──────────────────┘               └────────┬─────────┘
                                                                                    │ XADD stream:eeg
                                                                                    ▼
                                                                           ┌──────────────────┐
                                                                           │  Redis Streams   │
                                                                           │  (buffer)        │
                                                                           └────────┬─────────┘
                                                                                    │ XREAD
                                                                                    ▼
                                                                       ┌────────────────────────┐
                                                                       │  Modal Training Job    │
                                                                       │  1D CNN (PyTorch)      │
                                                                       │  → saves model.pt      │
                                                                       └────────────┬───────────┘
                                                                                    │ deploy
                                                                                    ▼
                                                                       ┌────────────────────────┐
                                                                       │  Modal Web Endpoint    │
                                                                       │  POST /predict         │
                                                                       │  returns: class +      │
                                                                       │  confidence + latency  │
                                                                       └────────────────────────┘
```

---

## Stack Decision Log

Every tool choice is a deliberate trade-off, not a default pick.

### FastAPI over Flask / Django
FastAPI has native async support and first-class WebSocket handling — critical for a streaming pipeline where connections are long-lived. Flask's WebSocket story requires bolt-on libraries (flask-socketio). Django is too heavy for a pure API + streaming service with no ORM needs.

### Redis Streams over Kafka
Kafka is the right answer at 10x scale (partitioned, replicated, consumer groups across machines). At this scale — one device sim, one backend, one training job — Kafka's operational overhead (ZooKeeper or KRaft, broker management, topic config) is pure cost with no benefit. Redis Streams gives the same append-only, consumer-group semantics with zero extra infrastructure. If this goes to production with 100+ devices, the migration path is clear: swap `XADD`/`XREAD` for a Kafka producer/consumer with the same message schema.

### Modal over AWS SageMaker / GCP Vertex
SageMaker requires IAM roles, VPC config, S3 buckets, and a significant cold start before you see a training log. Modal is Python-native: decorate a function, it runs on a GPU in the cloud — no broker management, no infra provisioning. SageMaker is the right choice when the org already lives in AWS and needs audit trails, VPCs, and enterprise billing.

> **Note:** Modal was chosen for speed of iteration. The training logic is cloud-agnostic and can be adapted to SageMaker or Vertex AI — the core `train()` function has no Modal-specific dependencies and can be wrapped in a different entrypoint without changes.

### PyTorch over TensorFlow / JAX
PyTorch is the standard in modern ML research and production ML infra. Imperative execution makes debugging straightforward, and its ecosystem — torchserve, ONNX export, Lightning — covers every direction this project could grow.

### MNE-Python for EEG simulation
MNE is the de facto standard library for EEG/MEG data in Python. It models real EEG characteristics — channel layouts, sampling rates, artifact profiles — so the simulated stream behaves like data from an actual device, not arbitrary noise.

### Batch training over online learning
Brain signal patterns need enough epochs to be statistically stable. Online learning (updating weights per window) risks overfitting to transient noise artifacts — a blink or jaw clench would corrupt the model weights in real-time. Batch training on accumulated windows gives stable gradient estimates and lets us validate on a held-out set before deploying. The intended trigger pattern is periodic retraining — e.g. every N minutes or after M windows accumulate — not a single training run, which keeps the model current without the instability of continuous updates.

---

## Folder Structure

```
eeg-stream/
├── device-sim/     # Synthetic EEG device (MNE-Python, WebSocket client)
├── backend/        # FastAPI server — WebSocket ingestion + Redis producer
├── model/          # 1D CNN definition + Modal training job
├── inference/      # Modal web endpoint + latency logger
└── docs/           # Architecture notes, decisions, diagrams
```

---

## Quickstart

```bash
# 1. Install dependencies
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Start Redis (Docker)
docker run -d -p 6379:6379 redis:7

# 3. Start backend
uvicorn backend.main:app --reload

# 4. Run device simulator
python device-sim/simulator.py

# 5. (Day 4+) Train on Modal
modal run model/train.py

# 6. (Day 5+) Deploy inference
modal deploy inference/endpoint.py
```

---

## Latency Target

`< 100ms` end-to-end from window ready → prediction returned.
Logged as p50 / p95 / p99 in `logs/latency.csv`.

---

## What breaks first at 10x scale?

Redis becomes the bottleneck — single-threaded write path, no replication. Fix: migrate to Kafka with partitioning by device ID. The WebSocket layer would also need horizontal scaling behind a load balancer with sticky sessions (or switch to a pub/sub broker). Modal inference would need `keep_warm > 1` to handle concurrent requests.
