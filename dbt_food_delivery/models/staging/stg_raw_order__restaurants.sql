select distinct
    raw_payload:restaurant:restaurant_id::string as restaurant_id,
    raw_payload:restaurant:name::string          as name,
    raw_payload:restaurant:cuisine_type::string as cuisine_type,
    raw_payload:restaurant:rating::float        as rating

from {{ source('raw', 'raw_order') }}
where raw_payload:event_type::string = 'order_created'
