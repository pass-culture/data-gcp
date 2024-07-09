select
    date(mqe.execution_date) as execution_date,
    dashboard_id,
    card_id,
    mrc.card_name,
    mrd.dashboard_name,
    count(distinct executor_id) as total_users,
    sum(running_time) as running_time,
    sum(result_rows) as result_rows,
    count(*) as total_views
from
    {{ source("raw", "metabase_query_execution") }} mqe
    JOIN {{ source("raw", "metabase_report_card") }}  mrc ON mqe.card_id = mrc.id
    LEFT JOIN {{ source("raw", "metabase_report_dashboard") }} mrd ON mqe.dashboard_id = mrd.id
GROUP BY
    1,
    2,
    3,
    4,
    5