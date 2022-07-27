select
    date(mqe.started_at) as execution_date,
    dashboard_id,
    card_id,
    mrc.card_name,
    mrd.dashboard_name,
    count(distinct executor_id) as total_users,
    sum(running_time) as running_time,
    sum(result_rows) as result_rows,
    count(*) as total_views
from
    `{{ bigquery_raw_dataset }}.metabase_query_execution` mqe
    JOIN `{{ bigquery_raw_dataset }}.metabase_report_card` mrc ON mqe.card_id = mrc.id
    LEFT JOIN `{{ bigquery_raw_dataset }}.metabase_report_dashboard` mrd ON mqe.dashboard_id = mrd.id
GROUP BY
    1,
    2,
    3,
    4,
    5