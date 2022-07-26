WITH bq_costs AS (
  SELECT
    project_id,
    user_email,
    cache_hit,
    date(creation_time) as creation_date,
    query,
    sum(total_bytes_processed) as total_bytes, 
    count(*) as total_queries
  FROM `{{ gcp_project }}`.`region-europe-west1`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
  GROUP BY 1, 2, 3, 4, 5

),

query_hash AS (
    SELECT *, 
        regexp_extract(query, r"Metabase:: userID: ([0-9]+).*") as metabase_user_id,
        regexp_extract(query, r"queryHash: ([a-z-0-9]+)\n") as metabase_hash,
        total_bytes * 5 / power(1024, 4) as cost_usd
    FROM bq_costs
),

metabase_costs AS (
    SELECT
        creation_date, 
        metabase_hash,
        metabase_user_id,
        SUM(total_bytes) as total_bytes,
        SUM(total_queries) as total_queries,
        SUM(cost_usd) as cost_usd
    FROM query_hash
    GROUP BY creation_date, metabase_hash, metabase_user_id
),


metabase_queries AS (
    SELECT 
        date(started_at) as date,
        TO_HEX(`hash`) as metabase_hash, 
        dashboard_id,
        card_id,
        executor_id as metabase_user_id,
        mrc.card_name,
        mrd.dashboard_name,
        sum(running_time) as running_time,
        sum(result_rows) as result_rows

    FROM `{{ bigquery_raw_dataset }}`.metabase_query_execution mqe
    JOIN `{{ bigquery_raw_dataset }}`.metabase_report_card mrc ON mqe.card_id = mrc.id
    AND NOT mqe.cache_hit 
    LEFT JOIN `{{ bigquery_raw_dataset }}`.metabase_report_dashboard mrd  ON mqe.dashboard_id = mrd.id
    GROUP BY 1,2,3,4,5, 6, 7

)


SELECT 
    mq.date as date,
    mq.metabase_hash, 
    mq.dashboard_id,
    mq.card_id,
    mq.metabase_user_id,
    mq.card_name,
    mq.dashboard_name,
    result_rows,
    running_time,
    total_bytes,
    total_queries, 
    cost_usd
FROM metabase_queries mq 
LEFT JOIN metabase_costs mc on mc.creation_date = mq.date 
    AND mc.metabase_hash = mq.metabase_hash
    AND CAST(mc.metabase_user_id AS INT) = mq.metabase_user_id