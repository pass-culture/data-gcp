{{
    config(
        **custom_incremental_config(
            incremental_strategy = "insert_overwrite",
            partition_by = {"field": "first_date", "data_type": "date", "granularity": "day"},
            on_schema_change = "sync_all_columns",
        )
    )
}}

WITH 

-- Step 1: Extracting Consulted Offers
consulted_offers AS (
    SELECT
        unique_session_id,
        user_id,
        offer_id,
        search_id,
        unique_search_id,
        event_timestamp AS consult_timestamp,
        event_date AS consult_date
    FROM {{ ref('int_firebase__native_event') }}
    WHERE
        event_name = 'ConsultOffer'
        AND search_id IS NOT NULL
        {% if is_incremental() %}
            AND event_date BETWEEN DATE_SUB(DATE('{{ ds() }}'), INTERVAL 3 DAY) AND DATE('{{ ds() }}')
        {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_session_id, offer_id, unique_search_id ORDER BY event_timestamp) = 1
),

-- Step 2: Extracting Booked Offers
booked_offers AS (
    SELECT
        co.unique_session_id,
        co.user_id,
        co.search_id,
        co.unique_search_id,
        co.offer_id,
        co.consult_timestamp,
        db.delta_diversification
    FROM consulted_offers co
    JOIN {{ ref('int_firebase__native_event') }} ne
        ON co.unique_session_id = ne.unique_session_id
        AND co.offer_id = ne.offer_id
        AND ne.event_name = 'BookingConfirmation'
        AND ne.event_timestamp > co.consult_timestamp
        {% if is_incremental() %}
            AND ne.event_date BETWEEN DATE_SUB(DATE('{{ ds() }}'), INTERVAL 3 DAY) AND DATE('{{ ds() }}')
        {% endif %}
    LEFT JOIN {{ ref('diversification_booking') }} db
        ON db.user_id = co.user_id
        AND db.offer_id = co.offer_id
        AND DATE(co.consult_timestamp) = DATE(db.booking_creation_date)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY co.unique_search_id, co.unique_session_id, co.offer_id ORDER BY ne.event_timestamp) = 1
),

-- Step 3: Aggregating Bookings by Search ID
bookings_aggregated AS (
    SELECT DISTINCT
        search_id,
        unique_search_id,
        unique_session_id,
        user_id,
        COUNT(DISTINCT offer_id) OVER (PARTITION BY unique_search_id, unique_session_id) AS nb_offers_booked,
        SUM(delta_diversification) OVER (PARTITION BY unique_search_id, unique_session_id) AS total_diversification
    FROM booked_offers
),

-- Step 4: Identifying the First Search Performed
first_search AS (
    SELECT
        search_id,
        unique_session_id,
        unique_search_id,
        CASE
            WHEN query IS NOT NULL THEN 'text_input'
            WHEN search_categories_filter IS NOT NULL THEN 'categories_filter'
            WHEN search_genre_types_filter IS NOT NULL THEN 'genre_types_filter'
            WHEN search_native_categories_filter IS NOT NULL THEN 'native_categories_filter'
            WHEN search_date_filter IS NOT NULL THEN 'date_filter'
            WHEN search_max_price_filter IS NOT NULL THEN 'max_price_filter'
            WHEN search_location_filter IS NOT NULL THEN 'location_filter'
            WHEN search_accessibility_filter IS NOT NULL THEN 'accessibility_filter'
            ELSE 'Autre'
        END AS first_filter_applied
    FROM {{ ref('int_firebase__native_event') }}
    WHERE
        event_name = 'PerformSearch'
        AND unique_search_id IS NOT NULL
        {% if is_incremental() %}
            AND event_date BETWEEN DATE_SUB(DATE('{{ ds() }}'), INTERVAL 3 DAY) AND DATE('{{ ds() }}')
        {% endif %}
        AND NOT (
            event_name = 'PerformSearch'
            AND search_type = 'Suggestions'
            AND query IS NULL
            AND search_categories_filter IS NULL
            AND search_native_categories_filter IS NULL
            AND search_location_filter = '{"locationType":"EVERYWHERE"}'
        )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_search_id, unique_session_id ORDER BY event_timestamp) = 1
),

-- Step 5: Identifying the Last Search Performed
last_search AS (
    SELECT
        unique_search_id,
        unique_session_id,
        event_date AS first_date,
        event_timestamp AS first_timestamp,
        app_version,
        query AS query_input,
        search_type,
        search_date_filter,
        search_location_filter,
        search_categories_filter,
        search_genre_types_filter,
        search_max_price_filter,
        search_is_autocomplete,
        search_is_based_on_history,
        search_offer_is_duo_filter,
        search_native_categories_filter,
        search_accessibility_filter,
        user_location_type
    FROM {{ ref('int_firebase__native_event') }}
    WHERE
        event_name = 'PerformSearch'
        {% if is_incremental() %}
            AND event_date BETWEEN DATE_SUB(DATE('{{ ds() }}'), INTERVAL 3 DAY) AND DATE('{{ ds() }}')
        {% endif %}
        AND NOT (
            event_name = 'PerformSearch'
            AND search_type = 'Suggestions'
            AND query IS NULL
            AND search_categories_filter IS NULL
            AND search_native_categories_filter IS NULL
            AND search_location_filter = '{"locationType":"EVERYWHERE"}'
        )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_search_id, unique_session_id ORDER BY event_timestamp DESC) = 1
),

-- Step 6: Conversion Metrics per Search
conversion_metrics AS (
    SELECT
        ls.unique_session_id,
        ls.unique_search_id,
        COUNT(DISTINCT CASE WHEN ne.event_name = 'ConsultOffer' THEN ne.offer_id END) AS nb_offers_consulted,
        COUNT(DISTINCT CASE WHEN ne.event_name = 'HasAddedOfferToFavorites' THEN ne.offer_id END) AS nb_offers_added_to_favorites,
        COUNT(CASE WHEN ne.event_name = 'NoSearchResult' THEN 1 END) AS nb_no_search_result,
        COUNT(CASE WHEN ne.event_name = 'PerformSearch' THEN 1 END) AS nb_iterations_search,
        COUNT(CASE WHEN ne.event_name = 'VenuePlaylistDisplayedOnSearchResults' THEN 1 END) AS nb_venue_playlist_displayed_on_search_results,
        COUNT(DISTINCT CASE WHEN ne.event_name = 'ConsultVenue' THEN ne.venue_id END) AS nb_venues_consulted
    FROM last_search ls
    LEFT JOIN {{ ref('int_firebase__native_event') }} ne
        ON 
        ne.event_name IN ('NoSearchResult', 'ConsultOffer', 'HasAddedOfferToFavorites', 'VenuePlaylistDisplayedOnSearchResults', 'ConsultVenue')
        AND ne.unique_session_id = ls.unique_session_id
        AND ne.unique_search_id = ls.unique_search_id
        AND ne.event_date = ls.first_date 
        
    GROUP BY ls.unique_session_id, ls.unique_search_id
),

-- Step 7: Aggregating All Search Data
agg_search_data AS (
    SELECT
        ls.*,
        fs.first_filter_applied,
        cm.* EXCEPT(unique_search_id, unique_session_id)
    FROM last_search ls
    INNER JOIN first_search fs USING (unique_search_id, unique_session_id)
    INNER JOIN conversion_metrics cm USING (unique_search_id, unique_session_id)
),

-- Step 8: Categorizing Search Type and Calculating Additional Metrics
final_data AS (
    SELECT
        asd.*,
        LEAD(first_date) OVER (PARTITION BY unique_session_id ORDER BY first_timestamp) IS NOT NULL AS made_another_search,
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM {{ source('clean', 'subcategories') }} sc
                WHERE LOWER(asd.query_input) LIKE CONCAT('%', LOWER(sc.category_id), '%')
                    OR LOWER(asd.query_input) LIKE CONCAT('%', LOWER(sc.id), '%')
            )
            OR EXISTS (
                SELECT 1
                FROM {{ source('seed', 'macro_rayons') }} mr
                WHERE LOWER(asd.query_input) LIKE CONCAT('%', LOWER(mr.macro_rayon), '%')
                    OR LOWER(asd.query_input) LIKE CONCAT('%', LOWER(mr.rayon), '%')
            )
            THEN TRUE
            ELSE FALSE
        END AS search_query_input_is_generic,
        bpsi.nb_offers_booked,
        bpsi.total_diversification
    FROM agg_search_data asd
    LEFT JOIN bookings_aggregated bpsi USING (unique_search_id, unique_session_id)
)

SELECT
    unique_search_id,
    unique_session_id,
    first_date,
    first_timestamp,
    app_version,
    query_input,
    search_type,
    search_date_filter,
    search_location_filter,
    search_categories_filter,
    search_genre_types_filter,
    search_max_price_filter,
    search_is_autocomplete,
    search_is_based_on_history,
    search_offer_is_duo_filter,
    search_native_categories_filter,
    search_accessibility_filter,
    user_location_type,
    first_filter_applied,
    nb_offers_consulted,
    nb_offers_added_to_favorites,
    nb_no_search_result,
    nb_iterations_search,
    nb_venue_playlist_displayed_on_search_results,
    nb_venues_consulted,
    made_another_search,
    search_query_input_is_generic,
    nb_offers_booked,
    total_diversification,
    CASE
        WHEN query_input IS NOT NULL AND NOT search_query_input_is_generic THEN 'specific_search'
        ELSE 'discovery_search'
    END AS search_objective
FROM final_data