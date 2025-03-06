{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={
                "field": "execution_date",
                "data_type": "date",
                "granularity": "day",
            },
            on_schema_change="append_new_columns",
        )
    )
}}

with
    consult_per_offer_last_30_days as (
        select offerer_id, offer_id, sum(cnt_events) as nb_consult_last_30_days
        from {{ ref("aggregated_daily_offer_consultation_data") }}
        where
            event_date
            between date_sub(current_date, interval 30 day) and date(current_date)
            and event_name = 'ConsultOffer'
        group by 1, 2
    )

select
    date('{{ ds() }}') as execution_date,
    offerer_id,
    offer_id,
    nb_consult_last_30_days,
    row_number() over (
        partition by offerer_id order by nb_consult_last_30_days desc
    ) as consult_rank
from consult_per_offer_last_30_days
qualify
    row_number() over (partition by offerer_id order by nb_consult_last_30_days desc)
    <= 3
