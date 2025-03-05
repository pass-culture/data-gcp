SELECT
    event_id,
    event_title,
    offerer_id,
    venue_id,
    event_creation_date,
    event_date,
    event_response_id,
    user_id,
    response_status,
    response_submitted_date
FROM {{ ref("int_applicative__special_event") }}
