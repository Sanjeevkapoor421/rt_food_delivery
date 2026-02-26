import snowflake.connector
import json
import os
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )

def insert_event(raw_payload, partition, offset):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        insert_query ="""
            INSERT INTO raw_order
            (raw_payload, kafka_partition, kafka_offset)
            SELECT PARSE_JSON(%s), %s, %s
            """
        cursor.execute(
            insert_query,
            (json.dumps(raw_payload), partition, offset)
        )
        conn.commit()
    finally:
        cursor.close()
        conn.close()
