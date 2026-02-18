from kafka import KafkaConsumer
import json
from .snowflake_writer import insert_order

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="order-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

for message in consumer:
    order = message.value
    print("Received:", order)

    insert_order(order)
    print("Inserted into Snowflake")
