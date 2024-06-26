{{
    config(
        materialized = "incremental" if target.profile != "CI" else "view",
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "event_date", "data_type": "date", "granularity" : "day"},
        on_schema_change = "sync_all_columns",
    )
}}

SELECT DISTINCT
        event_date,
        user_pseudo_id,
        user_id,
        user_properties.key as experiment_name,
        user_properties.value.string_value as experiment_value
FROM {{ source('raw','firebase_events') }},
UNNEST(user_properties) AS user_properties
where user_properties.key like "%firebase_exp%"
    {% if is_incremental() %}
    AND event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 1 DAY) and DATE("{{ ds() }}")
    {% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY user_pseudo_id,user_id,experiment_name ORDER BY event_date DESC) = 1
