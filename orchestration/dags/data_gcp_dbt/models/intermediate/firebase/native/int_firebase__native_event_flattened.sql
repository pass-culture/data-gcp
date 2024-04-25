{{
    config(
        materialized = "incremental",
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "event_date", "data_type": "date"},
        on_schema_change = "sync_all_columns"
    )
}}

WITH firebase_last_two_days_events AS (
    SELECT *
    FROM {{ source("raw","firebase_events") }}
    WHERE TRUE
        {% if is_incremental() %}
        AND event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 3 DAY) and DATE("{{ ds() }}")
        {% endif %}
)

SELECT
    event_date,
    user_id,
    user_pseudo_id,
    event_name,
    TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    TIMESTAMP_MICROS(event_previous_timestamp) AS event_previous_timestamp,
    TIMESTAMP_MICROS(event_timestamp) AS user_first_touch_timestamp,
    platform,
    traffic_source.name,
    traffic_source.medium,
    traffic_source.source,
    app_info.version AS app_version,
    (SELECT event_params.value.double_value from unnest(event_params) event_params where event_params.key = 'offerId') as double_offer_id,
    {{ extract_params_int_value(["ga_session_id",
                                    "ga_session_number",
                                    "shouldUseAlgoliaRecommend",
                                    "searchIsAutocomplete",
                                    "searchIsBasedOnHistory",
                                    "searchOfferIsDuo",
                                    "geo_located",
                                    "enabled"
                                    ])}}
    {{ extract_params_string_value([
                                "searchId",
                                "moduleId",
                                "moduleName",
                                "playlistType",
                                "entryId",
                                "homeEntryId",
                                "locationType",
                                "step",
                                "searchGenreTypes",
                                "call_id",
                                "searchLocationFilter",
                                "age",
                                "searchCategories",
                                "firebase_screen",
                                "firebase_previous_screen",
                                "pageName",
                                "offerId",
                                "query",
                                "searchQuery",
                                "categoryName",
                                "type",
                                "venueId",
                                "bookingId",
                                "fromOfferId",
                                "filterTypes",
                                "filter",
                                "searchDate",
                                "searchMaxPrice",
                                "searchNativeCategories",
                                "moduleListID",
                                "index",
                                "traffic_campaign",
                                "traffic_source",
                                "traffic_medium",
                                "traffic_gen",
                                "traffic_content",
                                "toEntryId",
                                "reco_origin",
                                "ab_test",
                                "model_version",
                                "model_name",
                                "model_endpoint",
                                "userStatus",
                                "social",
                                "searchView",
                                "duration",
                                "appsFlyerUserId",
                                "accessibilityFilter"
                                ])
                                }},
    (SELECT event_params.value.string_value from unnest(event_params) event_params where event_params.key = 'from') as origin
FROM firebase_last_two_days_events
