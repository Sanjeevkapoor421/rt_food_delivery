from kafka import KafkaProducer
import json
import time
from faker import Faker

fake = Faker()


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

while True:
    order = {
        "order_id": fake.random_int(),
        "city": fake.city(),
        "amount": round(fake.random_number(digits=3) / 10, 2),
        "timestamp": str(fake.date_time())
    }

    producer.send("orders", order)
    print("Sent:", order)

    time.sleep(2)
