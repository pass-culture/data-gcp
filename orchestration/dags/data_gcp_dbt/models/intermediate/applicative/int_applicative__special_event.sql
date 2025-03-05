select
    ser.event_id,
    se.event_title,
    se.offerer_id,
    se.venue_id,
    ser.event_response_id,
    ser.user_id,
    ser.response_status,
    date(se.event_creation_date) as event_creation_date,
    date(se.event_date) as event_date,
    date(ser.response_submitted_date) as response_submitted_date
from {{ ref("raw_applicative__special_event_response") }} as ser
left join
    {{ ref("raw_applicative__special_event") }} as se on ser.event_id = se.event_id
