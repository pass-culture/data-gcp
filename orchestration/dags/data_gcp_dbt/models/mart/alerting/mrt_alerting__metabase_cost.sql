{{
    config(
        materialized="table",
        tags=["weekly"],
        labels={"schedule": "weekly"},
    )
}}

with
    temp_card as (
        select distinct
            card_id,
            dashboard_name,
            row_number() over (partition by card_id order by date desc) as rank,
            max(date) over (partition by card_id) as consultation_date
        from {{ ref("mrt_monitoring__metabase_cost") }}
        where dashboard_id is not null
    )

select distinct
    cost.card_name,
    cost.card_id,
    temp_card.dashboard_name,
    sum(cost.total_queries) over (partition by cost.card_id) as total_views,
    count(distinct cost.metabase_user_id) over (
        partition by cost.card_id
    ) as total_distinct_users,
    sum(cost.cost_euro) over (partition by cost.card_id) as total_cost,
    avg(cost.cost_euro) over (partition by cost.card_id) as avg_cost
from {{ ref("mrt_monitoring__metabase_cost") }} as cost
left join temp_card on cost.card_id = temp_card.card_id and temp_card.rank = 1
where
    cost.card_id is not null
    and lower(cost.card_name) not like '%archive%'
    and temp_card.consultation_date >= date_sub(current_date(), interval 7 day)
