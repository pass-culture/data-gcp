{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "event_date", "data_type": "date"},
            on_schema_change="sync_all_columns"
        )
    )
}}

SELECT DISTINCT
    event_date,
    user_pseudo_id,
    user_id,
    (
        SELECT value.int_value
        FROM UNNEST(user_properties) AS up
        WHERE up.key = 'offerer_id'
    ) AS offerer_id,
    event_params.key AS experiment_name,
    SAFE_CAST(event_params.value.string_value AS BOOLEAN) AS experiment_value
FROM 
    {{ source("raw", "firebase_pro_events") }},
    UNNEST(event_params) AS event_params
WHERE
    (
        event_params.key = "PRO_DIDACTIC_ONBOARDING_AB_TEST"
        OR event_params.key LIKE "PRO_EXPERIMENT%"
    )
    AND event_params.value.string_value IS NOT NULL
    {% if is_incremental() %}
        AND event_date BETWEEN DATE_SUB(DATE("{{ ds() }}"), INTERVAL 1 DAY)
        AND DATE("{{ ds() }}")
    {% else %} 
        AND event_date >= DATE_SUB(DATE("{{ ds() }}"), INTERVAL 6 MONTH)
    {% endif %}

