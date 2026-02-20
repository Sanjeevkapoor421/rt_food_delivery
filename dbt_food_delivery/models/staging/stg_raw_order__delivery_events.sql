select
    raw_payload:order_id::string            as order_id,
    raw_payload:delivery_time_minutes::int  as delivery_time_minutes,
    raw_payload:event_timestamp::timestamp_ltz as event_time

from {{ source('raw', 'raw_order') }}
where raw_payload:event_type::string = 'order_delivered'
