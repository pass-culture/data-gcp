select
    special_event_id,
    special_event_title,
    offerer_id,
    venue_id,
    special_event_creation_date,
    special_event_date,
    special_event_response_id,
    user_id,
    special_event_response_status,
    special_event_response_submitted_date
from {{ ref("int_applicative__special_event") }}
