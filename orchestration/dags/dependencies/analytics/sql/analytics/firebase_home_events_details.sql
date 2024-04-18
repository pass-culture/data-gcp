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
        booking_id,
        module_name,
        content_type,
        module_id,
        home_id,
        module_index
    FROM `{{ bigquery_analytics_dataset }}.firebase_home_events`
    WHERE date(event_date) >= DATE_SUB(date('{{ ds }}'), INTERVAL 12 MONTH)

), 
contentful_tags AS (
    SELECT 
        entry_id,
        ARRAY_TO_STRING(
        array_agg(
            coalesce(playlist_type, 'temporaire')
            ), " / ") as playlist_type
    FROM `{{ bigquery_int_contentful_dataset }}.tags` 
    GROUP BY entry_id
), 
diversification_booking AS (
    -- in case we have more than one reservation for the same offer in the same day, take average (this should not happen).
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
    ebd.booking_is_cancelled,
    CASE WHEN 
        event_type = "booking" THEN db.delta_diversification
    ELSE NULL
    END AS  delta_diversification,
    if(event_type = "booking" AND db.delta_diversification is not null, 1, 0) as effective_booking,
    -- current data
    eud.user_department_code,
    eud.user_region_name,
    eud.user_current_deposit_type,
    eud.user_age,
    CASE 
        WHEN eud.user_current_deposit_type = 'GRANT_18' THEN 'Bénéficiaire 18-20 ans'
        WHEN eud.user_current_deposit_type = 'GRANT_15_17' THEN 'Bénéficiaire 15-17 ans'
        WHEN e.user_id IS NOT NULL THEN 'Grand Public'
    ELSE 'Non connecté' end as user_role

FROM firebase_home_events e
LEFT JOIN contentful_tags contentful_tags on contentful_tags.entry_id = e.module_id
LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_user_data` eud ON e.user_id = eud.user_id
LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_booking_data` ebd ON e.booking_id = ebd.booking_id
LEFT JOIN diversification_booking db
    ON db.user_id = e.user_id
    AND db.offer_id = e.offer_id
    AND db.date = e.event_date
