{{
    config(
        **custom_incremental_config(
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "execution_date", "data_type": "date", "granularity" : "day"},
        on_schema_change = "sync_all_columns",
    )
) }}

with
CONSULT_PER_OFFER_LAST_3O_DAYS as (
    select
        OFFERER_ID,
        OFFER_ID,
        SUM(CNT_EVENTS) as NB_CONSULT_LAST_30_DAYS
    from
        {{ ref('aggregated_daily_offer_consultation_data') }}
    where
        EVENT_DATE between DATE_SUB(CURRENT_DATE, interval 30 day) and DATE(CURRENT_DATE)
        and
        EVENT_NAME = 'ConsultOffer'
    group by
        1,
        2
)

select
    DATE('{{ ds() }}') as EXECUTION_DATE,
    OFFERER_ID,
    OFFER_ID,
    NB_CONSULT_LAST_30_DAYS,
    ROW_NUMBER() over (partition by OFFERER_ID order by NB_CONSULT_LAST_30_DAYS desc) as CONSULT_RANK
from
    CONSULT_PER_OFFER_LAST_3O_DAYS
qualify ROW_NUMBER() over (partition by OFFERER_ID order by NB_CONSULT_LAST_30_DAYS desc) <= 3
