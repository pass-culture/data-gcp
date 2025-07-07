{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "event_date", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

select distinct
    event_date,
    user_pseudo_id,
    user_id,
    event_params.key as experiment_name,
    (
        select value.int_value
        from unnest(user_properties) as up
        where up.key = 'offerer_id'
    ) as offerer_id,
    safe_cast(event_params.value.string_value as boolean) as experiment_value
from {{ source("raw", "firebase_pro_events") }}, unnest(event_params) as event_params
where
    (
        event_params.key = 'PRO_DIDACTIC_ONBOARDING_AB_TEST'
        or event_params.key like 'PRO_EXPERIMENT%'
        or event_params.key = 'AB_COLLECTIVE_DESCRIPTION_TEMPLATE'
    )
    and event_params.value.string_value is not null
    {% if is_incremental() %}
        and event_date
        between date_sub(date("{{ ds() }}"), interval 1 day) and date("{{ ds() }}")
    {% else %} and event_date >= date_sub(date("{{ ds() }}"), interval 6 month)
    {% endif %}
