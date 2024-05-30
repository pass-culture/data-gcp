WITH
consulted_from_search AS (
    SELECT
        unique_session_id
        , user_id
        , offer_id
        , search_id
        , unique_search_id
        , event_timestamp AS consult_timestamp
        , event_date AS consult_date
    FROM `{{ bigquery_int_firebase_dataset }}`.native_event
    WHERE event_date > DATE('{{ params.set_date }}')
    AND event_name = 'ConsultOffer'
    AND origin = 'search'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY unique_session_id, offer_id, unique_search_id ORDER BY event_timestamp) = 1
),
booked_from_search AS (
    SELECT
        consulted_from_search.unique_session_id
        , consulted_from_search.user_id
        , consulted_from_search.search_id
        , consulted_from_search.unique_search_id
        , consulted_from_search.offer_id
        , consult_timestamp
        , delta_diversification
    FROM consulted_from_search
    JOIN `{{ bigquery_int_firebase_dataset }}`.native_event
        ON consulted_from_search.unique_session_id = firebase_events.unique_session_id
        AND consulted_from_search.offer_id = firebase_events.offer_id
        AND event_date > DATE('{{ params.set_date }}')
        AND event_name = 'BookingConfirmation'
        AND event_timestamp > consult_timestamp
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.diversification_booking
        ON diversification_booking.user_id = consulted_from_search.user_id
        AND diversification_booking.offer_id = consulted_from_search.offer_id
        AND DATE(consult_timestamp) = DATE(booking_creation_date)
    QUALIFY ROW_NUMBER() OVER(PARTITION BY consulted_from_search.unique_search_id,consulted_from_search.unique_session_id, consulted_from_search.offer_id ORDER BY event_timestamp) = 1

    ),
bookings_per_search_id AS (
    SELECT DISTINCT
        search_id
        , unique_search_id
        , unique_session_id
        , user_id
        , COUNT(DISTINCT offer_id) OVER(PARTITION BY unique_search_id, unique_session_id) AS nb_offers_booked
        , SUM(delta_diversification) OVER(PARTITION BY unique_search_id, unique_session_id) AS total_diversification
    FROM booked_from_search
),

first_search AS ( -- Lors de la 1ère itération de cette recherche, filtre appliqué
SELECT
    search_id
    , unique_session_id
    , unique_search_id
    , CASE
        WHEN query IS NOT NULL THEN 'text_input'
        WHEN search_categories_filter IS NOT NULL THEN 'categories_filter'
        WHEN search_genre_types_filter IS NOT NULL THEN 'genre_types_filter'
        WHEN search_native_categories_filter IS NOT NULL THEN 'native_categories_filter'
        WHEN search_date_filter IS NOT NULL THEN 'date_filter'
        WHEN search_max_price_filter IS NOT NULL THEN 'max_price_filter'
        WHEN search_location_filter IS NOT NULL THEN 'location_filter'
        WHEN search_accessibility_filter IS NOT NULL THEN 'accessibility_filter'
        ELSE 'Autre' END AS first_filter_applied
FROM `{{ bigquery_int_firebase_dataset }}`.native_event
WHERE event_date > DATE('{{ params.set_date }}')
AND event_name = 'PerformSearch'
AND unique_search_id IS NOT NULL
AND NOT (event_name = 'PerformSearch' AND search_type = 'Suggestions' AND query IS NULL AND search_categories_filter IS NULL AND search_native_categories_filter IS NULL AND search_location_filter = '{"locationType":"EVERYWHERE"}')
QUALIFY ROW_NUMBER() OVER(PARTITION BY unique_search_id, unique_session_id ORDER BY event_timestamp) = 1
),

last_search AS ( -- Lors de la dernière itération de cette recherche, le filtre appliqué
SELECT
    unique_search_id
    , unique_session_id
    , event_date AS first_date
    , event_timestamp AS first_timestamp
    , app_version
    , query AS query_input
    , search_type
    , search_date_filter
    , search_location_filter
    , search_categories_filter
    , search_genre_types_filter
    , search_max_price_filter
    , search_is_autocomplete
    , search_is_based_on_history
    , search_offer_is_duo_filter
    , search_native_categories_filter
    , search_accessibility_filter
    , user_location_type
 FROM `{{ bigquery_int_firebase_dataset }}`.native_event
 WHERE event_name = 'PerformSearch'
 AND NOT (event_name = 'PerformSearch' AND search_type = 'Suggestions' AND query IS NULL AND search_categories_filter IS NULL AND search_native_categories_filter IS NULL AND search_location_filter = '{"locationType":"EVERYWHERE"}')
QUALIFY ROW_NUMBER() OVER(PARTITION BY unique_search_id, unique_session_id ORDER BY event_timestamp DESC) = 1
 ),

conversion_per_search  AS (

SELECT
    last_search.unique_session_id, last_search.unique_search_id
    , COUNT(DISTINCT CASE WHEN event_name = 'ConsultOffer' THEN offer_id ELSE NULL END)  AS nb_offers_consulted
    , COUNT(DISTINCT CASE WHEN event_name = 'HasAddedOfferToFavorites' THEN offer_id ELSE NULL END)  AS nb_offers_added_to_favorites
    , COUNT( CASE WHEN event_name = 'NoSearchResult' THEN 1 ELSE NULL END)  AS nb_no_search_result
    , COUNT( CASE WHEN event_name = 'PerformSearch' THEN 1 ELSE NULL END)  AS nb_iterations_search
    , COUNT( CASE WHEN event_name = 'VenuePlaylistDisplayedOnSearchResults' THEN 1 ELSE NULL END)  AS nb_venue_playlist_displayed_on_search_results
    , COUNT( DISTINCT CASE WHEN event_name = 'ConsultVenue' THEN venue_id ELSE NULL END)   AS nb_venues_consulted
FROM last_search
LEFT JOIN `{{ bigquery_int_firebase_dataset }}`.native_event ON firebase_events.unique_session_id = last_search.unique_session_id
                                        AND firebase_events.unique_search_id = last_search.unique_search_id
                                        AND event_name IN ('NoSearchResult','ConsultOffer','HasAddedOfferToFavorites','VenuePlaylistDisplayedOnSearchResults','ConsultVenue')
GROUP BY 1,2
),

agg_search_data AS (
SELECT
  last_search.*
  , first_search.* EXCEPT(unique_search_id, unique_session_id)
  , conversion_per_search.* EXCEPT(unique_search_id, unique_session_id)
FROM last_search
INNER JOIN first_search USING(unique_search_id, unique_session_id)
INNER JOIN conversion_per_search USING(unique_search_id, unique_session_id)
),

generic_search_and_other_searches AS (
SELECT
    agg_search_data.*
    , LEAD(first_date) OVER(PARTITION BY unique_session_id ORDER BY first_timestamp) IS NOT NULL AS made_another_search
    ,  CASE
    WHEN EXISTS ( -- Si recherche textuelle contient une catégorie d'offre, alors la recherche est considérée comme générique
      SELECT 1
      FROM `{{ bigquery_analytics_dataset }}`.subcategories sc
      WHERE LOWER(query_input) LIKE CONCAT('%', LOWER(sc.category_id), '%')
         OR LOWER(query_input) LIKE CONCAT('%', LOWER(sc.id), '%') )
    OR EXISTS ( -- Si recherche textuelle contient un rayon / macro rayon, alors la recherche est considérée comme générique
      SELECT 1
      FROM `{{ bigquery_analytics_dataset }}`.macro_rayons mr
      WHERE LOWER(query_input) LIKE CONCAT('%', LOWER(mr.macro_rayon), '%')
         OR LOWER(query_input) LIKE CONCAT('%', LOWER(mr.rayon), '%')
    ) THEN TRUE
    ELSE FALSE
  END AS search_query_input_is_generic
    , nb_offers_booked
    , total_diversification
FROM agg_search_data
LEFT JOIN bookings_per_search_id USING (unique_search_id, unique_session_id)
)

SELECT
  *
  , CASE
    WHEN query_input IS NOT NULL AND NOT search_query_input_is_generic THEN 'specific_search'
    ELSE 'discovery_search' END AS search_objective
FROM generic_search_and_other_searches
