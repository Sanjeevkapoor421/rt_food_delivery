select distinct
    raw_payload:user:user_id::string as user_id,
    raw_payload:user:city::string    as city,
    raw_payload:user:zipcode::string as zipcode

from {{ source('raw', 'raw_order') }}
where raw_payload:event_type::string = 'order_created'
