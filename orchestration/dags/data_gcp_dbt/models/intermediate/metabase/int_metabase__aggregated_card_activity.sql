SELECT 
    card_id
    , sum(running_time) as total_running_time
    , sum(result_rows) as total_result_rows
    , avg(running_time) as avg_running_time
    , avg(result_rows) as avg_result_rows
    , count(distinct metabase_user_id) as total_users
    , count(distinct execution_id) as total_views
    , count(distinct dashboard_id) as nbr_dashboards
    , max(execution_date) as last_execution_date
    , sum(case when error is null then 0 else 1 end) as total_errors
FROM  {{ ref('int_metabase__daily_query') }} as execution_query
GROUP BY 
    card_id