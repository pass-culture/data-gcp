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
    FROM {{ source("raw","events_test") }}
    {% if is_incremental() %}
    WHERE event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 2 DAY) and DATE("{{ ds() }}")
    {% endif %}

)

SELECT
    event_date,
    TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    TIMESTAMP_MICROS(event_previous_timestamp) AS event_previous_timestamp,
    user_id,
    user_pseudo_id,
    event_name,
    MAX(platform) AS platform,
    MAX(app_info.version) AS app_version,
    MAX(traffic_source.source) AS traffic_source,
    MAX(traffic_source.medium) AS traffic_medium,
    MAX(traffic_source.name) AS traffic_campaign,
    MAX(CASE WHEN params.key = "offerId" THEN params.value.double_value END) AS double_offer_id,
    {{ extract_params_int_value(["ga_session_id",
                                    "ga_session_number",
                                    "shouldUseAlgoliaRecommend",
                                    "searchIsAutocomplete",
                                    "searchIsBasedOnHistory",
                                    "searchOfferIsDuo",
                                    "geo_located",
                                    "enabled"
                                    ])}},
    {{ extract_params_string_value(["firebase_screen",
                                "firebase_previous_screen",
                                "pageName",
                                "offerId",
                                "locationType",
                                "query",
                                "searchQuery",
                                "categoryName",
                                "type",
                                "venueId",
                                "bookingId",
                                "fromOfferId",
                                "playlistType",
                                "step",
                                "filterTypes",
                                "searchId",
                                "filter",
                                "searchLocationFilter",
                                "searchCategories",
                                "searchDate",
                                "searchGenreTypes",
                                "searchMaxPrice",
                                "searchNativeCategories",
                                "moduleName",
                                "moduleId",
                                "moduleListID",
                                "index",
                                "traffic_gen",
                                "traffic_content",
                                "entryId",
                                "homeEntryId",
                                "toEntryId",
                                "reco_origin",
                                "ab_test",
                                "call_id",
                                "model_version",
                                "model_name",
                                "model_endpoint",
                                "age",
                                "userStatus",
                                "social",
                                "searchView",
                                "duration",
                                "appsFlyerUserId"
                                ])
                                }},
        MAX(CASE WHEN params.key = "from" THEN params.value.string_value END) AS origin
FROM firebase_last_two_days_events,
    UNNEST(event_params) AS params
GROUP BY 1, 2, 3, 4, 5, 6
