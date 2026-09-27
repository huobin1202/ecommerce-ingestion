import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


TOPIC = "ecommerce-events"
BOOTSTRAP_SERVERS = "localhost:9092"

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
        json.dumps(value).encode("utf-8")
)


def generate_event():

    event_type = random.choice(EVENT_TYPES)

    event = {
        "event_id": f"EVT-{uuid.uuid4().hex[:12]}",
        "event_type": event_type,
        "user_id": f"U-{random.randint(1, 10000):05d}",
        "product_id": f"P-{random.randint(1, 1000):05d}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "device": random.choice(DEVICES),
        "session_id": f"S-{uuid.uuid4().hex[:12]}"
    }

    return event


print("Starting E-commerce Event Generator...")
print(f"Kafka: {BOOTSTRAP_SERVERS}")
print(f"Topic: {TOPIC}")


try:

    while True:

        event = generate_event()

        producer.send(
            TOPIC,
            value=event
        )

        print(
            f"Sent | "
            f"{event['event_id']} | "
            f"{event['event_type']} | "
            f"{event['user_id']}"
        )

        time.sleep(0.1)

except KeyboardInterrupt:

    print("\nStopping producer...")

finally:

    producer.flush()
    producer.close()