import json

from kafka import KafkaConsumer


TOPIC = "ecommerce-events"
BOOTSTRAP_SERVERS = "localhost:9092"


consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    group_id="demo-consumer",
    value_deserializer=lambda value:
        json.loads(value.decode("utf-8"))
)


print("Waiting for events...")


for message in consumer:

    event = message.value

    print(
        f"Received | "
        f"partition={message.partition} | "
        f"offset={message.offset} | "
        f"{event}"
    )