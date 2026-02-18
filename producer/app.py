import json
import random
import time
import uuid
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

fake = Faker()

# Kafka config
TOPIC = "order_events"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def current_timestamp():
    return datetime.utcnow().isoformat()


def generate_order():
    order_id = f"ORD_{uuid.uuid4().hex[:8]}"
    user_id = f"USR_{random.randint(1000, 9999)}"
    restaurant_id = f"RES_{random.randint(100, 999)}"

    total_amount = round(random.uniform(15, 60), 2)
    delivery_fee = round(random.uniform(2, 5), 2)
    tax_amount = round(total_amount * 0.07, 2)
    discount_amount = round(random.choice([0, 5, 10]), 2)

    items = []
    for _ in range(random.randint(1, 3)):
        price = round(random.uniform(5, 15), 2)
        quantity = random.randint(1, 2)

        items.append({
            "item_id": f"IT_{random.randint(100,999)}",
            "item_name": fake.word().capitalize(),
            "category": random.choice(["Main", "Beverage", "Dessert"]),
            "quantity": quantity,
            "price_per_unit": price
        })

    return {
        "event_type": "order_created",
        "event_timestamp": current_timestamp(),
        "order": {
            "order_id": order_id,
            "order_status": "CREATED",
            "total_amount": total_amount,
            "delivery_fee": delivery_fee,
            "tax_amount": tax_amount,
            "discount_amount": discount_amount,
            "currency": "EUR"
        },
        "user": {
            "user_id": user_id,
            "city": fake.city(),
            "zipcode": fake.postcode()
        },
        "restaurant": {
            "restaurant_id": restaurant_id,
            "name": fake.company(),
            "cuisine_type": random.choice(["Italian", "Indian", "American", "Asian"]),
            "rating": round(random.uniform(3.5, 5.0), 1)
        },
        "items": items,
        "payment": {
            "payment_id": f"PAY_{uuid.uuid4().hex[:6]}",
            "method": random.choice(["Card", "Cash", "UPI"]),
            "status": "PENDING"
        }
    }


def send_event(event):
    producer.send(TOPIC, value=event)
    producer.flush()
    print(f"Sent event: {event['event_type']} for order")


def simulate_lifecycle():
    order_event = generate_order()
    order_id = order_event["order"]["order_id"]

    # 1️⃣ Order Created
    send_event(order_event)

    time.sleep(random.randint(1, 3))

    # 2️⃣ Payment Decision
    if random.random() < 0.85:
        payment_event = {
            "event_type": "payment_success",
            "event_timestamp": current_timestamp(),
            "order_id": order_id,
            "payment_status": "SUCCESS"
        }
        send_event(payment_event)

        time.sleep(random.randint(1, 3))

        # 3️⃣ Delivery or Cancellation
        if random.random() < 0.9:
            delivery_event = {
                "event_type": "order_delivered",
                "event_timestamp": current_timestamp(),
                "order_id": order_id,
                "delivery_time_minutes": random.randint(20, 45)
            }
            send_event(delivery_event)
        else:
            cancel_event = {
                "event_type": "order_cancelled",
                "event_timestamp": current_timestamp(),
                "order_id": order_id,
                "cancel_reason": "Restaurant unavailable"
            }
            send_event(cancel_event)

    else:
        failed_event = {
            "event_type": "payment_failed",
            "event_timestamp": current_timestamp(),
            "order_id": order_id,
            "payment_status": "FAILED"
        }
        send_event(failed_event)


if __name__ == "__main__":
    while True:
        simulate_lifecycle()
        time.sleep(random.randint(2, 5))
