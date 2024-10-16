with
    last_edition_card as (
        select mr.timestamp, mr.user_id, mu.email, mr.model_id as card_id
        from {{ source("raw", "metabase_revision") }} mr
        left join {{ ref("int_metabase__user") }} as mu on mu.user_id = mr.user_id
        where model = "Card"
        qualify row_number() over (partition by card_id order by timestamp desc) = 1
    )

select
    eq.card_id,
    lec.email as last_editor_email,
    date(lec.timestamp) as last_edition_date,
    sum(running_time) as total_running_time,
    sum(result_rows) as total_result_rows,
    avg(running_time) as avg_running_time,
    avg(result_rows) as avg_result_rows,
    count(distinct metabase_user_id) as total_users,
    count(distinct execution_id) as total_views,
    count(distinct dashboard_id) as nbr_dashboards,
    max(execution_date) as last_execution_date,
    sum(case when error is null then 0 else 1 end) as total_errors
from {{ ref("int_metabase__daily_query") }} as eq
left join last_edition_card as lec on lec.card_id = eq.card_id
where not cache_hit

group by 1, 2, 3
