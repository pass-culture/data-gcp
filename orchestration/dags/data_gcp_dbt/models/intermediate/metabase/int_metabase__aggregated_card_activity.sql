WITH last_edition_card AS (
    SELECT 
        mr.timestamp,
        mr.user_id, 
        mu.email,
        mr.model_id as card_id
    FROM {{ source("raw", "metabase_revision") }} mr
    LEFT JOIN {{ ref('int_metabase__user') }} as mu on mu.user_id = mr.user_id
    WHERE model = "Card"
    QUALIFY ROW_NUMBER() OVER (PARTITION BY card_id ORDER BY timestamp DESC) = 1
)

SELECT 
    eq.card_id
    , lec.email as last_editor_email
    , DATE(lec.timestamp) as last_edition_date
    , sum(running_time) as total_running_time
    , sum(result_rows) as total_result_rows
    , avg(running_time) as avg_running_time
    , avg(result_rows) as avg_result_rows
    , count(distinct metabase_user_id) as total_users
    , count(distinct execution_id) as total_views
    , count(distinct dashboard_id) as nbr_dashboards
    , max(execution_date) as last_execution_date
    , sum(case when error is null then 0 else 1 end) as total_errors
FROM  {{ ref('int_metabase__daily_query') }} as eq
LEFT JOIN last_edition_card as lec on lec.card_id = eq.card_id
WHERE NOT cache_hit

GROUP BY 1,2,3