select
    raw_payload:order:order_id::string        as order_id,
    raw_payload:order:order_status::string    as order_status,
    raw_payload:order:total_amount::float     as total_amount,
    raw_payload:order:delivery_fee::float     as delivery_fee,
    raw_payload:order:tax_amount::float       as tax_amount,
    raw_payload:order:discount_amount::float  as discount_amount,
    raw_payload:order:currency::string        as currency
    
from {{ source('raw', 'raw_order') }}
where raw_payload:event_type::string = 'order_created'
