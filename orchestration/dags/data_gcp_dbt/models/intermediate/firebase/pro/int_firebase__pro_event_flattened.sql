{{
    config(
        materialized = "incremental",
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "event_date", "data_type": "date"},
        on_schema_change = "sync_all_columns"
    )
}}

WITH firebase_pro_last_two_days_events AS (
    SELECT *
    FROM {{ source("raw","firebase_pro_events") }}
    WHERE TRUE
        {% if is_incremental() %}
        AND event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 2 DAY) and DATE("{{ ds() }}")
        {% endif %}
)

SELECT 
    event_name,
    user_pseudo_id,
    user_id,
    platform,
    event_date,
    TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    MAX(device.category) AS category,
    MAX(device.operating_system) AS operating_system,
    MAX(device.operating_system_version) AS operating_system_version,
    MAX(device.web_info.browser) AS browser,
    MAX(device.web_info.browser_version) AS browser_version,
    MAX(CASE WHEN params.key = "offerId" THEN params.value.double_value END) AS double_offer_id,
    {{ extract_params_int_value(["ga_session_number",
                                "ga_session_id",
                                "offerer_id",
                                "offererId",
                                "page_number",
                                "offerId",
                                "categorieJuridiqueUniteLegale",
                                "venueId"
    ])}},
    {{ extract_params_string_value(["venue_id",
                                "page_title",
                                "page_location",
                                "page_referrer",
                                "offerType",
                                "saved",
                                "hasOnly6eAnd5eStudents",
                                "isEdition",
                                "isDraft",
                                "filled",
                                "filledWithErrors",
                                "traffic_campaign",
                                "traffic_medium",
                                "traffic_source"
    ])}},
    MAX(CASE WHEN params.key = "from" THEN params.value.string_value END) AS origin,
    MAX(CASE WHEN params.key = "to" THEN params.value.string_value END) AS destination,
FROM firebase_pro_last_two_days_events,
    UNNEST(event_params) AS params
GROUP BY
    event_name,
    user_pseudo_id,
    user_id,
    platform,
    event_date,
    event_timestamp
