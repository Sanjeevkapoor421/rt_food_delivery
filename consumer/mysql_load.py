import mysql.connector 
from kafka import KafkaConsumer
import json


# Create connection
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Kapoordev@4",
    database="sql_practice"
)

def insert_order(order):
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO orders_stream (order_id, city, amount, timestamp)
            VALUES (%s, %s, %s, %s)
            """,
            (
                order["order_id"],
                order["city"],
                order["amount"],
                order["timestamp"],
            ),
        )
        conn.commit()  
    finally:
        cursor.close()

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
    print("Inserted into MySQL")
    
