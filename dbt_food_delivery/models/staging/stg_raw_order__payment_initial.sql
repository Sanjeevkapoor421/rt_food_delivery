select
    raw_payload:order:order_id::string      as order_id,
    raw_payload:payment:payment_id::string  as payment_id,
    raw_payload:payment:method::string      as payment_method,
    raw_payload:payment:status::string      as initial_status

from {{ source('raw', 'raw_order') }}
where raw_payload:event_type::string = 'order_created'
