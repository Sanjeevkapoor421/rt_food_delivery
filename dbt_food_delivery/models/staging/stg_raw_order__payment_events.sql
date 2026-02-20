select
    raw_payload:order_id::string        as order_id,
    raw_payload:payment_status::string  as payment_status,
    raw_payload:event_timestamp::timestamp_ltz as event_time

from {{ source('raw', 'raw_order') }}
where raw_payload:event_type::string in ('payment_success','payment_failed')
