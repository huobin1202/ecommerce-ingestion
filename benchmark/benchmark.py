import json
import random
import time
import uuid

from datetime import datetime, timezone

from kafka import KafkaProducer


TOPIC = "ecommerce-events"
BOOTSTRAP_SERVERS = "localhost:9092"

TOTAL_EVENTS = 100_000 #1_000_000


EVENT_TYPES = [
    "product_view",
    "search",
    "add_to_cart",
    "remove_from_cart",
    "purchase",
    "login",
    "logout"
]

DEVICES = [
    "mobile",
    "web",
    "tablet"
]


producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda value:
        json.dumps(value).encode("utf-8"),
    acks=1
)


def generate_event():

    return {
        "event_id": f"EVT-{uuid.uuid4().hex[:12]}",
        "event_type": random.choice(EVENT_TYPES),
        "user_id": f"U-{random.randint(1, 10000):05d}",
        "product_id": f"P-{random.randint(1, 1000):05d}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "device": random.choice(DEVICES),
        "session_id": f"S-{uuid.uuid4().hex[:12]}"
    }


print(f"Sending {TOTAL_EVENTS:,} events...")

start_time = time.perf_counter()

for i in range(TOTAL_EVENTS):

    event = generate_event()

    producer.send(
        TOPIC,
        value=event
    )

producer.flush()

end_time = time.perf_counter()

duration = end_time - start_time

throughput = TOTAL_EVENTS / duration


print()
print("==============================")
print("Kafka Benchmark")
print("==============================")
print(f"Messages   : {TOTAL_EVENTS:,}")
print(f"Duration   : {duration:.2f} seconds")
print(f"Throughput : {throughput:,.2f} events/sec")
print("==============================")

producer.close()