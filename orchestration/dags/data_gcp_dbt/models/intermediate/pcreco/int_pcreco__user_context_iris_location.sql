{{
    config(
        **custom_incremental_config(
        incremental_strategy = 'insert_overwrite',
        partition_by = {'field': 'event_date', 'data_type': 'date', "granularity" : "day"},
        on_schema_change = "sync_all_columns"
    )
) }}

select
    event_date,
    user_id,
    user_context.user_iris_id,
    COUNT(distinct reco_call_id) as total_displayed_modules,
    COUNT(distinct offer_id) as total_displayed_offers
from {{ ref("int_pcreco__displayed_offer_event") }} pso
where
    user_context.user_iris_id is not NULL
    and user_context.user_is_geolocated
    and user_id != "-1"

    {% if is_incremental() %}
        and event_date between DATE_SUB(DATE('{{ ds() }}'), interval 3 day) and DATE('{{ ds() }}')
    {% endif %}


group by
    event_date,
    user_id,
    user_iris_id
