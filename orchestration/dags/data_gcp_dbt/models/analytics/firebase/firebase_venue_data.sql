with venue_data AS (
SELECT
    user_id
    , unique_session_id
    , event_name
    , event_date
    , event_timestamp
    , origin
    , offer_id
    , booking_id
    , venue_id
    , app_version
FROM {{ ref('firebase_events') }}
WHERE (event_name IN ('ConsultVenue','BookingConfirmation') OR (event_name = 'ConsultOffer' AND origin = 'venue'))
AND unique_session_id IS NOT NULL
  {% if params.dag_type == 'intraday' %}
  AND event_date = DATE('{{ ds }}')
  {% else %}
  AND event_date = DATE('{{ add_days(ds, -1) }}')
  {% endif %}
),

display AS (
SELECT
    unique_session_id
    , event_timestamp AS display_timestamp
    , event_date AS display_date
    , origin AS display_origin
    , venue_id
    , ROW_NUMBER() OVER(PARTITION BY unique_session_id, venue_id ORDER BY event_timestamp) AS venue_display_rank
FROM venue_data
WHERE event_name = 'ConsultVenue'
QUALIFY ROW_NUMBER() OVER(PARTITION BY unique_session_id, venue_id ORDER BY event_timestamp) = 1 -- keep first_display
),

consult_offer AS (
SELECT
    display.* EXCEPT(venue_display_rank)
    , offer_id
    , event_timestamp AS consult_offer_timestamp
    , ROW_NUMBER() OVER(PARTITION BY display.unique_session_id, display.venue_id, offer_id ORDER BY event_timestamp) AS consult_rank
FROM display
LEFT JOIN venue_data ON display.unique_session_id = venue_data.unique_session_id
                    AND display.venue_id = venue_data.venue_id
                    AND event_name = 'ConsultOffer'
                    AND venue_data.event_timestamp > display_timestamp
QUALIFY ROW_NUMBER() OVER(PARTITION BY unique_session_id, venue_id, offer_id ORDER BY event_timestamp) = 1 -- keep 1 consult max

)


SELECT
    consult_offer.* EXCEPT(consult_rank)
    , venue_data.booking_id
    , event_timestamp AS booking_timestamp
FROM consult_offer
LEFT JOIN venue_data ON venue_data.unique_session_id = consult_offer.unique_session_id
                AND venue_data.offer_id = consult_offer.offer_id
                AND venue_data.event_timestamp > consult_offer.consult_offer_timestamp
                AND event_name = 'BookingConfirmation'
