select
    execution_date,
    dashboard_id,
    card_id,
    card_name,
    dashboard_name,
    count(distinct metabase_user_id) as total_users,
    sum(running_time) as running_time,
    sum(result_rows) as result_rows,
    count(*) as total_views
from
    {{ ref("int_metabase__daily_query") }}
GROUP BY
    execution_date,
    dashboard_id,
    card_id,
    card_name,
    dashboard_name