WITH temp_firebase_events AS (
    SELECT
        event_name,
        user_pseudo_id,
        user_id,
        platform,
        traffic_source.name,
        traffic_source.medium,
        traffic_source.source,
        app_info.version AS app_version,
        PARSE_DATE("%Y%m%d", event_date) AS event_date,
        TIMESTAMP_SECONDS(
            CAST(CAST(event_timestamp as INT64) / 1000000 as INT64)
        ) AS event_timestamp,
        TIMESTAMP_SECONDS(
            CAST(
                CAST(event_previous_timestamp as INT64) / 1000000 as INT64
            )
        ) AS event_previous_timestamp,
        TIMESTAMP_SECONDS(
            CAST(CAST(event_timestamp as INT64) / 1000000 as INT64)
        ) AS user_first_touch_timestamp,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'firebase_screen'
        ) as firebase_screen,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'firebase_previous_screen'
        ) as firebase_previous_screen,
        (
            select
                event_params.value.int_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'ga_session_number'
        ) as session_number,
        (
            select
                event_params.value.int_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'ga_session_id'
        ) as session_id,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'pageName'
        ) as page_name,
        (
            select
                CAST(event_params.value.double_value AS STRING)
            from
                unnest(event_params) event_params
            where
                event_params.key = 'offerId'
        ) as double_offer_id,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'offerId'
        ) as string_offer_id,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'from'
        ) as origin,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'query'
        ) as query,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'categoryName'
        ) as category_name,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'type'
        ) as filter_type,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'venueId'
        ) as venue_id,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'bookingId'
        ) as booking_id,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'fromOfferId'
        ) as similar_offer_id,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'filterTypes'
        ) as search_filter_types,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'searchId'
        ) as search_id,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'filter'
        ) as filter,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'searchLocationFilter'
        ) as search_location_filter,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'searchCategories'
        ) as search_categories_filter,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'searchDate'
        ) as search_date_filter,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'searchGenreTypes'
        ) as search_genre_types_filter,
        (
            select
                event_params.value.int_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'searchIsAutocomplete'
        ) as search_is_autocomplete,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'searchMaxPrice'
        ) as search_max_price_filter,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'searchNativeCategories'
        ) as search_native_categories_filter,
        (
            select
                event_params.value.int_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'searchOfferIsDuo'
        ) as search_offer_is_duo_filter,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'moduleName'
        ) as module_name,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'moduleId'
        ) as module_id,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'moduleListID'
        ) as module_list_id,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'index'
        ) as module_index,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'traffic_campaign'
        ) as traffic_campaign,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'traffic_medium'
        ) as traffic_medium,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'traffic_source'
        ) as traffic_source,
        COALESCE(
            (
                select
                    event_params.value.string_value
                from
                    unnest(event_params) event_params
                where
                    event_params.key = 'entryId'
            ),
            (
                select
                    event_params.value.string_value
                from
                    unnest(event_params) event_params
                where
                    event_params.key = 'homeEntryId'
            )
        ) AS entry_id,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'toEntryId'
        ) as destination_entry_id,
        CASE
            WHEN (
                select
                    event_params.value.string_value
                from
                    unnest(event_params) event_params
                where
                    event_params.key = 'entryId'
            ) IN (
                '4XbgmX7fVVgBMoCJiLiY9n',
                '1ZmUjN7Za1HfxlbAOJpik2'
            ) THEN "generale"
            WHEN (
                select
                    event_params.value.string_value
                from
                    unnest(event_params) event_params
                where
                    event_params.key = 'entryId'
            ) IS NULL THEN NULL
            ELSE "marketing"
        END AS home_type,
        -- recommendation
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'reco_origin'
        ) as reco_origin,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'ab_test'
        ) as reco_ab_test,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'call_id'
        ) as reco_call_id,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'model_version'
        ) as reco_model_version,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'model_name'
        ) as reco_model_name,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'model_endpoint'
        ) as reco_model_endpoint,
        (
            select
                event_params.value.int_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'geo_located'
        ) as reco_geo_located,
        -- ?
        (
            select
                event_params.value.int_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'enabled'
        ) as enabled,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'age'
        ) as onboarding_user_selected_age
FROM

        {% if params.dag_type == 'intraday' %}
        `{{ bigquery_clean_dataset }}.firebase_events_{{ yyyymmdd(ds) }}`
        {% else %}
        `{{ bigquery_clean_dataset }}.firebase_events_{{ yyyymmdd(add_days(ds, -1)) }}`
        {% endif %}
)
SELECT
    *
EXCEPT
(double_offer_id, string_offer_id),
    (
        CASE
            WHEN double_offer_id IS NULL THEN string_offer_id
            ELSE double_offer_id
        END
    ) AS offer_id
FROM
    temp_firebase_events