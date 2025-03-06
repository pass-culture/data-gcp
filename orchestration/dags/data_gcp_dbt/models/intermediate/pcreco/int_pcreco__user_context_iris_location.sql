{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={
                "field": "event_date",
                "data_type": "date",
                "granularity": "day",
            },
            on_schema_change="append_new_columns",
        )
    )
}}

select
    event_date,
    user_id,
    user_context.user_iris_id,
    count(distinct reco_call_id) as total_displayed_modules,
    count(distinct offer_id) as total_displayed_offers
from {{ ref("int_pcreco__displayed_offer_event") }} pso
where
    user_context.user_iris_id is not null
    and user_context.user_is_geolocated
    and user_id != "-1"

    {% if is_incremental() %}
        and event_date
        between date_sub(date('{{ ds() }}'), interval 3 day) and date('{{ ds() }}')
    {% endif %}

group by event_date, user_id, user_iris_id
