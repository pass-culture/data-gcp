WITH consulted_from_search AS (
SELECT
    user_pseudo_id
    , session_id
    , offer_id
    , search_id
    , event_timestamp AS consult_timestamp
FROM `{{ bigquery_analytics_dataset }}.firebase_events
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
JOIN `{{ bigquery_analytics_dataset }}.firebase_events ON consulted_from_search.user_pseudo_id = firebase_events.user_pseudo_id
                                    AND consulted_from_search.session_id = firebase_events.session_id
                                    AND consulted_from_search.offer_id = firebase_events.offer_id
                                    AND event_timestamp > consult_timestamp
                                    AND event_date > '2023-01-01'
                                    AND event_name = 'BookingConfirmation'
),



bookings_per_search_id AS (
SELECT
    search_id
    , COUNT(DISTINCT offer_id) AS nb_offers_booked
FROM booked_from_search
GROUP BY 1
),

agg_search_data AS (
SELECT
    search_id
    , user_id
    , user_pseudo_id
    , app_version
    , ANY_VALUE(session_id) AS session_id
    , MAX(query) AS query_input
    , MIN(event_date) AS first_date
    , MIN(event_timestamp) AS first_timestamp
    , MAX(search_date_filter ) search_date_filter
    ,MAX(search_categories_filter) search_categories_filter
    ,MAX(search_genre_types_filter) search_genre_types_filter
    ,MAX(search_is_autocomplete) search_is_autocomplete
    ,MAX(search_offer_is_duo_filter) search_offer_is_duo_filter
    ,MAX(search_native_categories_filter) search_native_categories_filter
    , COUNT(DISTINCT CASE WHEN event_name = 'ConsultOffer' THEN offer_id ELSE NULL END) AS nb_offers_consulted
    , COUNT( CASE WHEN event_name = 'NoSearchResult' THEN 1 ELSE NULL END) AS nb_no_search_result
FROM `{{ bigquery_analytics_dataset }}.firebase_events
WHERE event_name IN ('PerformSearch', 'NoSearchResult','ConsultOffer')
AND event_date > '2023-01-01'
AND search_id IS NOT NULL
GROUP BY 1,2,3,4
)

SELECT
    agg_search_data.*
    , nb_offers_booked
FROM agg_search_data
LEFT JOIN bookings_per_search_id USING (search_id)