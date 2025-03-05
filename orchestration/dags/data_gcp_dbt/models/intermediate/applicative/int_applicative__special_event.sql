SELECT
    ser.event_id,
    se.event_title,
    se.offerer_id,
    se.venue_id,
    ser.event_response_id,
    ser.user_id,
    ser.response_status,
    DATE(se.event_creation_date) AS event_creation_date,
    DATE(se.event_date) AS event_date,
    DATE(ser.response_submitted_date) AS response_submitted_date
FROM {{ ref("raw_applicative__special_event_response") }} AS ser
LEFT JOIN {{ ref("raw_applicative__special_event") }} AS se ON ser.event_id = se.event_id
