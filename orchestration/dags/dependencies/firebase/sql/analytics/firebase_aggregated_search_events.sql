WITH consulted_from_search AS (
SELECT
    user_pseudo_id
    , session_id
    , offer_id
    , search_id
    , event_timestamp AS consult_timestamp
FROM `{{ bigquery_analytics_dataset }}`.firebase_events
WHERE event_date > '2023-01-01'
AND event_name = 'ConsultOffer'
AND origin = 'search'),

booked_from_search AS (
SELECT
    consulted_from_search.user_pseudo_id
    ,consulted_from_search.session_id
    , consulted_from_search.search_id
    , consulted_from_search.offer_id
    , consult_timestamp
FROM consulted_from_search
JOIN `{{ bigquery_analytics_dataset }}`.firebase_events ON consulted_from_search.user_pseudo_id = firebase_events.user_pseudo_id
                                    AND consulted_from_search.session_id = firebase_events.session_id
                                    AND consulted_from_search.offer_id = firebase_events.offer_id
                                    AND event_date > '2023-01-01'
                                    AND event_name = 'BookingConfirmation'
                                    AND event_timestamp > consult_timestamp
),



bookings_per_search_id AS (
SELECT
    search_id
    , COUNT(DISTINCT offer_id) AS nb_offers_booked
FROM booked_from_search
GROUP BY 1
),

agg_search_data AS (
SELECT DISTINCT
    search_id
    , user_id
    , user_pseudo_id
    , app_version
    , LAST_VALUE(session_id) OVER (PARTITION BY search_id, user_id, user_pseudo_id ORDER BY event_timestamp) AS session_id
    , LAST_VALUE(query IGNORE NULLS) OVER (PARTITION BY search_id, user_id, user_pseudo_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS query_input
    , MIN(event_date) OVER (PARTITION BY search_id, user_id, user_pseudo_id) AS first_date
    , MIN(event_timestamp)OVER (PARTITION BY search_id, user_id, user_pseudo_id) AS first_timestamp
    , LAST_VALUE(search_date_filter IGNORE NULLS ) OVER (PARTITION BY search_id, user_id, user_pseudo_id ORDER BY event_timestamp) AS search_date_filter
    ,LAST_VALUE(search_categories_filter IGNORE NULLS) OVER (PARTITION BY search_id, user_id, user_pseudo_id ORDER BY event_timestamp) AS  search_categories_filter
    ,LAST_VALUE(search_genre_types_filter IGNORE NULLS) OVER (PARTITION BY search_id, user_id, user_pseudo_id ORDER BY event_timestamp) AS  search_genre_types_filter
    ,LAST_VALUE(search_is_autocomplete IGNORE NULLS) OVER (PARTITION BY search_id, user_id, user_pseudo_id ORDER BY event_timestamp) AS  search_is_autocomplete
    ,LAST_VALUE(search_offer_is_duo_filter IGNORE NULLS) OVER (PARTITION BY search_id, user_id, user_pseudo_id ORDER BY event_timestamp) search_offer_is_duo_filter
    ,LAST_VALUE(search_native_categories_filter IGNORE NULLS) OVER(PARTITION BY search_id, user_id, user_pseudo_id ORDER BY event_timestamp) search_native_categories_filter
    , COUNT(DISTINCT CASE WHEN event_name = 'ConsultOffer' THEN offer_id ELSE NULL END) OVER (PARTITION BY search_id, user_id, user_pseudo_id) AS nb_offers_consulted
    , COUNT( CASE WHEN event_name = 'NoSearchResult' THEN 1 ELSE NULL END) OVER (PARTITION BY search_id, user_id, user_pseudo_id ) AS nb_no_search_result
FROM `{{ bigquery_analytics_dataset }}`.firebase_events
WHERE event_name IN ('PerformSearch', 'NoSearchResult','ConsultOffer')
AND event_date > '2023-01-01'
AND search_id IS NOT NULL
)

SELECT
    agg_search_data.*
    , nb_offers_booked
FROM agg_search_data
LEFT JOIN bookings_per_search_id USING (search_id)
