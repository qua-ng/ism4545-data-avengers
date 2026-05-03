"""
MediaWave Kafka Producer — Stage 3
===================================
Produces fake user-activity events to the 'mediawave-user-activity' Kafka topic.

Run this BEFORE executing the Stage 3 notebook so Spark Structured Streaming
has events to read.

Usage (inside the Docker network, e.g. from the Jupyter terminal):
    pip install kafka-python
    python producers/mediawave_producer.py

The producer sends 200 events with a deliberate mix of actions and
a built-in poor-experience pattern (> 3 buffering events on two sessions)
so every streaming query in the notebook fires visible output.
"""

from kafka import KafkaProducer
import json
import time
import random

# ── configuration ────────────────────────────────────────────────────────────

BOOTSTRAP = "kafka:9092"
TOPIC     = "mediawave-user-activity"
N_EVENTS  = 200
DELAY     = 0.3   # seconds between events

# ── seed data ────────────────────────────────────────────────────────────────

titles = [
    ("t001", "Stranger Things S4"),
    ("t002", "The Bear S2"),
    ("t003", "House of the Dragon S1"),
    ("t004", "Severance S2"),
    ("t005", "Andor S1"),
    ("t006", "The Last of Us S1"),
    ("t007", "Abbott Elementary S3"),
    ("t008", "Succession S4"),
]

devices  = ["smart_tv", "mobile", "desktop", "tablet", "game_console"]
actions  = ["play", "pause", "browse", "skip", "search", "buffering"]
weights  = [0.35, 0.15, 0.20, 0.10, 0.10, 0.10]

# Two sessions intentionally accumulate > 3 buffering events
# so the poor-experience alert in Step 6 fires quickly.
poor_sessions = ["sess-bad-001", "sess-bad-002"]

# General session pool
session_pool = [f"sess-{i:04d}" for i in range(1, 21)] + poor_sessions

# ── producer ─────────────────────────────────────────────────────────────────

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None,
)

buffering_counts = {s: 0 for s in session_pool}

print(f"Producing {N_EVENTS} events to '{TOPIC}'...")

for i in range(N_EVENTS):
    content_id, title = random.choice(titles)

    # Force buffering on the poor sessions every ~8 events
    if i % 8 == 0 and i > 0:
        session_id = random.choice(poor_sessions)
        action     = "buffering"
    else:
        session_id = random.choice(session_pool)
        action     = random.choices(actions, weights=weights, k=1)[0]

    if action == "buffering":
        buffering_counts[session_id] += 1

    event = {
        "event_id":   f"ev-{i:06d}",
        "user_id":    f"u{random.randint(1000, 9999)}",
        "session_id": session_id,
        "event_ts":   time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
        "action":     action,
        "content_id": content_id,
        "title":      title,
        "device":     random.choice(devices),
    }

    producer.send(TOPIC, key=session_id, value=event)

    if (i + 1) % 50 == 0:
        poor = sum(1 for s in poor_sessions if buffering_counts[s] >= 3)
        print(f"  Sent {i + 1}/{N_EVENTS} events | "
              f"poor-experience sessions (>= 3 buffers): {poor}")

    time.sleep(DELAY)

producer.flush()
producer.close()

print(f"\nDone. {N_EVENTS} events sent to '{TOPIC}'.")
print(f"Poor-experience sessions (>= 3 buffers): "
      f"{', '.join(s for s in poor_sessions if buffering_counts[s] >= 3)}")
