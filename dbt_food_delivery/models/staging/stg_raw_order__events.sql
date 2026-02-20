select
    raw_payload:event_type::string             as event_type,
    raw_payload:event_timestamp::timestamp_ltz as event_time,

    coalesce(
        raw_payload:order:order_id::string,
        raw_payload:order_id::string
    ) as order_id,

    kafka_partition,
    kafka_offset,
    ingestion_timestamp

from {{ source('raw', 'raw_order') }}
