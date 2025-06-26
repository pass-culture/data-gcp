select
    ser.special_event_id,
    se.special_event_title,
    se.offerer_id,
    se.venue_id,
    se.typeform_id,
    ser.special_event_response_id,
    ser.user_id,
    ser.special_event_response_status,
    date(se.special_event_creation_date) as special_event_creation_date,
    date(se.special_event_date) as special_event_date,
    date(
        ser.special_event_response_submitted_date
    ) as special_event_response_submitted_date
from {{ ref("raw_applicative__special_event_response") }} as ser
left join
    {{ ref("raw_applicative__special_event") }} as se
    on ser.special_event_id = se.special_event_id
