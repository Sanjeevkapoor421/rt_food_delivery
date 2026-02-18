

SELECT 
    raw_payload:event_type::STRING AS event_type,
    raw_payload:event_timestamp::TIMESTAMP_LTZ AS event_time,
    kafka_partition,
    kafka_offset,
    ingestion_timestamp
FROM {{ source('raw','raw_order_events') }}
