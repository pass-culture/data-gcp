WITH firebase_home_events AS (
    SELECT
        event_date,
        event_timestamp,
        session_id,
        user_id,
        user_pseudo_id,
        call_id,
        platform,
        event_name,
        event_type,
        offer_id,
        module_name,
        content_type,
        module_id,
        home_id,
        module_index
    FROM `{{ bigquery_analytics_dataset }}.firebase_home_events`
), 
contentful_tags AS (
    SELECT 
        entry_id,
        ARRAY_TO_STRING(
        array_agg(
            coalesce(playlist_type, 'temporaire')
            ), " / ") as playlist_type
    FROM `{{ bigquery_analytics_dataset }}.contentful_tags` 
    GROUP BY entry_id
), 
diversification_booking AS (
    -- in case we have more than one reservation for the same offer in the same day, take average.
    SELECT
        DATE(booking_creation_date) as date,
        user_id,
        offer_id,
        avg(delta_diversification) as delta_diversification 
    FROM `{{ bigquery_analytics_dataset }}.diversification_booking` 
    GROUP BY 1,2,3 
)

SELECT
    e.*,
    coalesce(contentful_tags.playlist_type, 'temporaire') as playlist_type,
    CASE WHEN 
        event_type = "booking" THEN db.delta_diversification
    ELSE NULL
    END AS  delta_diversification,
    if(event_type = "booking" AND db.delta_diversification is not null, 1, 0) as effective_booking,
    -- current data
    eud.user_department_code,
    eud.user_region_name,
    eud.user_current_deposit_type,
    eud.user_age
FROM firebase_home_events e
LEFT JOIN contentful_tags contentful_tags on contentful_tags.entry_id = e.module_id
LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_user_data` eud ON e.user_id = eud.user_id
LEFT JOIN diversification_booking db 
    ON db.user_id = e.user_id
    AND db.offer_id = e.offer_id
    AND db.date = e.event_date
