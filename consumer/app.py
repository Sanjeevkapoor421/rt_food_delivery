from kafka import KafkaConsumer
import json
import logging
from .snowflake_writer import insert_event

logging.basicConfig(level=logging.INFO)


consumer = KafkaConsumer(
    "order_events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="order-event-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)
logging.info("Consumer started... Waiting for messages.")


for message in consumer:
    event = message.value
    partition = message.partition
    offset = message.offset
    logging.info(
            f"Received event_type={event.get('event_type')} "
            f"partition={partition} offset={offset}"
        )

    insert_event(
            raw_payload=event,
            partition=partition,
            offset=offset
        )
    logging.info("Inserted into Snowflake successfully.")

