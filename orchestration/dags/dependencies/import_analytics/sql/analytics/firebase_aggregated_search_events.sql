WITH
consulted_from_search AS (
    SELECT
        user_pseudo_id
        , user_id
        , session_id
        , offer_id
        , search_id
        , unique_search_id AS unique_search_id
        , event_timestamp AS consult_timestamp
        , event_date AS consult_date
        , ROW_NUMBER() OVER(PARTITION BY user_pseudo_id, session_id, offer_id, search_id ORDER BY event_timestamp) AS consult_rank
    FROM `{{ bigquery_analytics_dataset }}`.firebase_events
    WHERE event_date > DATE('{{ params.set_date }}')
    AND event_name = 'ConsultOffer'
    AND origin = 'search'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY user_pseudo_id, session_id, offer_id, search_id ORDER BY event_timestamp) = 1
),
booked_from_search AS (
    SELECT
        consulted_from_search.user_pseudo_id
        , consulted_from_search.session_id
        , consulted_from_search.search_id
        , consulted_from_search.unique_search_id
        , consulted_from_search.offer_id
        , consult_timestamp
        , delta_diversification
        , ROW_NUMBER() OVER(PARTITION BY consulted_from_search.unique_search_id, consulted_from_search.offer_id ORDER BY event_timestamp) AS book_rank
    FROM consulted_from_search
    JOIN `{{ bigquery_analytics_dataset }}`.firebase_events
        ON consulted_from_search.user_pseudo_id = firebase_events.user_pseudo_id
        AND consulted_from_search.session_id = firebase_events.session_id
        AND consulted_from_search.offer_id = firebase_events.offer_id
        AND event_date > DATE('{{ params.set_date }}')
        AND event_name = 'BookingConfirmation'
        AND event_timestamp > consult_timestamp
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.diversification_booking
        ON diversification_booking.user_id = consulted_from_search.user_id
        AND diversification_booking.offer_id = consulted_from_search.offer_id
        AND DATE(consult_timestamp) = DATE(booking_creation_date)
    QUALIFY ROW_NUMBER() OVER(PARTITION BY consulted_from_search.unique_search_id, consulted_from_search.offer_id ORDER BY event_timestamp) = 1

    ),
bookings_per_search_id AS (
    SELECT DISTINCT
        search_id
        , unique_search_id
        , user_pseudo_id
        , session_id
        , COUNT(DISTINCT offer_id) OVER(PARTITION BY unique_search_id) AS nb_offers_booked
        , SUM(delta_diversification) OVER(PARTITION BY unique_search_id) AS total_diversification
    FROM booked_from_search
),
agg_search_data AS (
SELECT DISTINCT
    search_id
    , unique_search_id AS unique_search_id
    , user_id
    , user_pseudo_id
    , app_version
    , LAST_VALUE(session_id) OVER (PARTITION BY unique_search_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS session_id
    , LAST_VALUE(query IGNORE NULLS) OVER (PARTITION BY unique_search_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS query_input
    , MIN(event_date) OVER (PARTITION BY unique_search_id) AS first_date
    , MIN(event_timestamp)OVER (PARTITION BY unique_search_id) AS first_timestamp
    , LAST_VALUE(search_type IGNORE NULLS ) OVER (PARTITION BY unique_search_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS search_type
    , LAST_VALUE(search_date_filter IGNORE NULLS ) OVER (PARTITION BY unique_search_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS search_date_filter
    , LAST_VALUE(search_location_filter IGNORE NULLS ) OVER (PARTITION BY unique_search_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS search_location_filter
    ,LAST_VALUE(search_categories_filter IGNORE NULLS) OVER (PARTITION BY unique_search_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS  search_categories_filter
    ,LAST_VALUE(search_genre_types_filter IGNORE NULLS) OVER (PARTITION BY unique_search_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS  search_genre_types_filter
    ,LAST_VALUE(search_max_price_filter IGNORE NULLS) OVER (PARTITION BY unique_search_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS  search_max_price_filter
    ,LAST_VALUE(search_is_autocomplete IGNORE NULLS) OVER (PARTITION BY unique_search_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS  search_is_autocomplete
    ,LAST_VALUE(search_offer_is_duo_filter IGNORE NULLS) OVER (PARTITION BY unique_search_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) search_offer_is_duo_filter
    ,LAST_VALUE(search_native_categories_filter IGNORE NULLS) OVER(PARTITION BY unique_search_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) search_native_categories_filter
    , COUNT(DISTINCT CASE WHEN event_name = 'ConsultOffer' THEN offer_id ELSE NULL END) OVER (PARTITION BY unique_search_id) AS nb_offers_consulted
    , COUNT(DISTINCT CASE WHEN event_name = 'HasAddedOfferToFavorites' THEN offer_id ELSE NULL END) OVER (PARTITION BY unique_search_id) AS nb_offers_added_to_favorites
    , COUNT( CASE WHEN event_name = 'NoSearchResult' THEN 1 ELSE NULL END) OVER (PARTITION BY unique_search_id ) AS nb_no_search_result
    , COUNT( CASE WHEN event_name = 'PerformSearch' THEN 1 ELSE NULL END) OVER (PARTITION BY unique_search_id ) AS nb_iterations_search
    , COUNT( CASE WHEN event_name = 'VenuePlaylistDisplayedOnSearchResults' THEN 1 ELSE NULL END) OVER (PARTITION BY unique_search_id ) AS nb_venue_playlist_displayed_on_search_results
    , COUNT( DISTINCT CASE WHEN event_name = 'ConsultVenue' THEN venue_id ELSE NULL END) OVER (PARTITION BY unique_search_id ) AS nb_venues_consulted
    FROM `{{ bigquery_analytics_dataset }}`.firebase_events
WHERE event_name IN ('PerformSearch', 'NoSearchResult','ConsultOffer','HasAddedOfferToFavorites','VenuePlaylistDisplayedOnSearchResults','ConsultVenue')
AND event_date > DATE('{{ params.set_date }}')
AND search_id IS NOT NULL
)

SELECT
    agg_search_data.*
    , nb_offers_booked
    , total_diversification
FROM agg_search_data
LEFT JOIN bookings_per_search_id USING (unique_search_id)
