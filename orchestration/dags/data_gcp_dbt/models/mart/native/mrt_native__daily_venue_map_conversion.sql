{{
    config(
        **custom_incremental_config(
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "event_date", "data_type": "date", "granularity" : "day"},
        on_schema_change = "sync_all_columns",
    )
) }}

WITH venue_map_consultations AS ( -- Toutes les consultations d'offres
-- consulted from venue 
SELECT 
    offer.unique_session_id
    , offer.user_id
    , offer.event_date
    , offer.event_timestamp
    , offer.app_version
    , offer.event_name
    , offer.origin
    , offer.venue_id
    , offer.offer_id
    , offer.duration_seconds
FROM {{ ref('int_firebase__native_venue_map_event') }} offer
INNER JOIN {{ ref('int_firebase__native_venue_map_event') }} preview USING(unique_session_id, venue_id)
WHERE offer.event_name = 'ConsultOffer' AND offer.origin = 'venue'
AND preview.event_name = 'ConsultVenue'
AND preview.event_timestamp < offer.event_timestamp
    {% if is_incremental() %}
        and offer.event_date between DATE_SUB(DATE("{{ ds() }}"), interval 1 day) and DATE("{{ ds() }}")
    {% endif %}
QUALIFY ROW_NUMBER() OVER(PARTITION BY unique_session_id, offer_id ORDER BY event_timestamp) = 1
UNION ALL 
-- consulted directly from preview 
SELECT
    unique_session_id
    , user_id
    , event_date
    , event_timestamp
    , app_version
    , event_name
    , origin
    , venue_id
    , offer_id
    , duration_seconds
FROM {{ ref('int_firebase__native_venue_map_event') }}
WHERE event_name = 'ConsultOffer'
AND origin IN ("venuemap", "venueMap")
    {% if is_incremental() %}
        and event_date between DATE_SUB(DATE("{{ ds() }}"), interval 1 day) and DATE("{{ ds() }}")
    {% endif %}
QUALIFY ROW_NUMBER() OVER(PARTITION BY unique_session_id, offer_id ORDER BY event_timestamp) = 1
)

,venue_map_bookings AS ( -- Les réservations
SELECT 
    unique_session_id
    , ne.user_id
    , ne.event_date
    , ne.event_timestamp
    , ne.app_version
    , ne.event_name
    , ne.origin
    , ne.venue_id
    , ne.offer_id
    , SAFE_CAST(ne.duration AS INT64) AS duration_seconds
    , delta_diversification
FROM {{ ref('int_firebase__native_event') }} ne
INNER JOIN venue_map_consultations USING(unique_session_id, offer_id, user_id)
LEFT JOIN {{ ref('diversification_booking') }}  db ON db.booking_id = ne.booking_id
WHERE ne.event_name = 'BookingConfirmation'
AND venue_map_consultations.event_name = 'ConsultOffer'
and ne.event_timestamp > venue_map_consultations.event_timestamp
    {% if is_incremental() %}
        and ne.event_date between DATE_SUB(DATE("{{ ds() }}"), interval 1 day) and DATE("{{ ds() }}")
    {% endif %}
)

, all_events AS ( -- On reprend les events "enrichis"
SELECT 
    unique_session_id
    , user_id
    , event_date
    , event_timestamp
    , app_version
    , event_name
    , origin
    , venue_id
    , offer_id
    , duration_seconds
    , NULL AS delta_diversification
FROM {{ ref('int_firebase__native_venue_map_event') }}
WHERE event_name IN ("ConsultVenueMap", "VenueMapSessionDuration", "VenueMapSeenDuration","PinMapPressed", "ConsultVenue")
    {% if is_incremental() %}
        and event_date between DATE_SUB(DATE("{{ ds() }}"), interval 1 day) and DATE("{{ ds() }}")
    {% endif %}
UNION ALL 
SELECT 
    unique_session_id
    , user_id
    , event_date
    , event_timestamp
    , app_version
    , event_name
    , origin
    , venue_id
    , offer_id
    , duration_seconds
    , NULL AS delta_diversification
FROM venue_map_consultations
UNION ALL
SELECT 
    unique_session_id
    , user_id
    , event_date
    , event_timestamp
    , app_version
    , event_name
    , origin
    , venue_id
    , offer_id
    , duration_seconds
    , delta_diversification
FROM venue_map_bookings
)

SELECT 
    unique_session_id
    , user_id
    , app_version
    , MIN(event_date) AS event_date
    , COUNT(CASE WHEN event_name = 'ConsultVenueMap' THEN 1 END) AS total_venue_map_consult
    , COUNT(DISTINCT CASE WHEN event_name = 'PinMapPressed' THEN venue_id END) AS total_venue_map_preview
    , COUNT(DISTINCT CASE WHEN event_name = 'ConsultVenue' THEN venue_id END) AS total_consult_venue
    , COUNT(DISTINCT CASE WHEN event_name = 'ConsultOffer' THEN venue_id END) AS total_distinct_venue_consult_offer
    , COUNT(DISTINCT CASE WHEN event_name = 'ConsultOffer' THEN offer_id END) AS total_consult_offer
    , COUNT(DISTINCT CASE WHEN event_name = 'BookingConfirmation' THEN offer_id END) AS total_bookings
    , COUNT(DISTINCT CASE WHEN event_name = 'BookingConfirmation' AND delta_diversification IS NOT NULL THEN offer_id END) AS total_non_cancelled_bookings
    , SUM(COALESCE(delta_diversification,0)) AS total_diversification
    , SUM(CASE WHEN event_name = 'VenueMapSeenDuration' THEN duration_seconds END) AS total_session_venue_map_seen_duration_seconds
FROM all_events
GROUP BY 
    unique_session_id
    , user_id
    , app_version