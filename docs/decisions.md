# Architecture Decision Log

Quick-reference answers for verbal walkthrough. Each answer is one paragraph — say it out loud, time it.

---

## "Why Redis Streams over Kafka?"

"At this scale — one device, one backend, one training job — Kafka's operational overhead (broker management, ZooKeeper/KRaft, topic config) gives us nothing. Redis Streams has the same append-only, consumer-group semantics with zero extra infra. The migration path to Kafka is clear if we scale to 100+ devices: same XADD/XREAD interface maps directly to a Kafka producer/consumer. I'd make that call when the device count justifies it."

---

## "Why batch training over online learning?"

"Brain signals have transient noise — blinks, muscle artifacts, jaw movement. If we update model weights on every incoming window, a single noisy epoch corrupts the weights in real-time. Batch training on accumulated windows gives stable gradient estimates and lets us validate on a held-out set before deploying. The right production pattern is periodic retraining every N hours, not continuous online updates."

---

## "Why Modal over SageMaker?"

"SageMaker requires IAM roles, VPC config, S3 buckets, and a 10–15 minute cold start before you see a training log. Modal is Python-native: decorate a function, it runs on a GPU in the cloud. For a time-constrained project the goal is *proving the training works*, not managing infra. SageMaker is the right call when the org already lives in AWS and needs audit trails, VPCs, and enterprise billing."

---

## "What breaks first at 10x scale?"

"Redis — it's single-threaded writes, no replication by default. I'd migrate to Kafka partitioned by device ID. The WebSocket ingestion layer would need horizontal scaling behind a load balancer with sticky sessions, or a switch to a pub/sub broker. On inference, `keep_warm=1` handles one concurrent request — at 10x we'd bump that or put the endpoint behind an autoscaling policy."

---

## Talking points for the "no handholding" signal

- **Reconnection logic (Day 2):** Device sim auto-retries with exponential backoff. Dead-letter log for dropped packets. Most candidates ignore what happens when the device drops.
- **Artifact rejection (Day 3):** Flag windows where any channel exceeds 3 std devs — mark, don't discard. "Brain signals have noise from blinking and movement. I flag rather than silently discard so we keep a record of data quality."
- **Latency percentiles (Day 5):** p50/p95/p99 logged to CSV. "p50 tells you the median, p99 tells you what your worst users experience. Just saying 'it's fast' isn't a production answer."
