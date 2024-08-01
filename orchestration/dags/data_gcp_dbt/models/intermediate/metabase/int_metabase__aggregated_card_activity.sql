with last_edition_card as (
    select
        mr.timestamp,
        mr.user_id,
        mu.email,
        mr.model_id as card_id
    from {{ source("raw", "metabase_revision") }} mr
        left join {{ ref('int_metabase__user') }} as mu on mu.user_id = mr.user_id
    where model = "Card"
    qualify ROW_NUMBER() over (partition by card_id order by timestamp desc) = 1
)

select
    eq.card_id,
    lec.email as last_editor_email,
    DATE(lec.timestamp) as last_edition_date,
    SUM(running_time) as total_running_time,
    SUM(result_rows) as total_result_rows,
    AVG(running_time) as avg_running_time,
    AVG(result_rows) as avg_result_rows,
    COUNT(distinct metabase_user_id) as total_users,
    COUNT(distinct execution_id) as total_views,
    COUNT(distinct dashboard_id) as nbr_dashboards,
    MAX(execution_date) as last_execution_date,
    SUM(case when error is null then 0 else 1 end) as total_errors
from {{ ref('int_metabase__daily_query') }} as eq
    left join last_edition_card as lec on lec.card_id = eq.card_id
where not cache_hit

group by 1, 2, 3
