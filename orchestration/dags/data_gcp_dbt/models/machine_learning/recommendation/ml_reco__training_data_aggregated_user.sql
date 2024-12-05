{{ config(**custom_table_config(materialized="view")) }}

with
    base as (
        select *
        from {{ ref("ml_reco__training_data_booking") }}
        union all
        select *
        from {{ ref("ml_reco__training_data_click") }}
        union all
        select *
        from {{ ref("ml_reco__training_data_favorite") }}
        order by user_id
    )
select
    user_id,
    item_id,
    event_type,
    offer_subcategory_id,
    event_date,
    event_hour,
    event_day,
    event_month,
    count(*) as count
from base
group by
    user_id,
    item_id,
    event_type,
    offer_subcategory_id,
    event_date,
    event_hour,
    event_day,
    event_month
