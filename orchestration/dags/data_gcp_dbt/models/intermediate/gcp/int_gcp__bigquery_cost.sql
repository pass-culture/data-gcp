
{{
    config(
        **custom_incremental_config(
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "start_date", "data_type": "date"},
        on_schema_change = "sync_all_columns"
    )
) }}

WITH table_references AS (
  SELECT
    q.project_id,
    q.job_id,
    STRING_AGG(CONCAT(referenced_table_unn.dataset_id, '.', referenced_table_unn.table_id), "," ORDER BY CONCAT(referenced_table_unn.dataset_id, '.', referenced_table_unn.table_id) ) as referenced_tables,

  FROM `{{ target.project }}.{{ var('region_name') }}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT q,
  UNNEST(referenced_tables) AS referenced_table_unn

  GROUP BY 1, 2
),

bq_costs AS (
  SELECT
    date(start_time) as start_date,
    date(creation_time) as creation_date,
    queries.project_id,
    queries.job_id,
    user_email,
    cache_hit,
    destination_table.dataset_id as dataset_id,
    destination_table.table_id as table_id,
    tr.referenced_tables,
    statement_type,
    query,
    CAST(regexp_extract(query, r"Metabase:: userID: ([0-9]+).*") AS INT) as metabase_user_id,
    regexp_extract(query, r"queryHash: ([a-z-0-9]+)\n") as metabase_hash,
    sum(total_bytes_billed) as total_bytes_billed,
    sum(total_bytes_processed) as total_bytes_processed,
    count(*) as total_queries
  FROM `{{ target.project }}.{{ var('region_name') }}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT queries
  LEFT JOIN table_references tr ON queries.project_id = tr.project_id AND queries.job_id = tr.job_id
  {% if is_incremental() %}
    WHERE date(creation_time) BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 28 DAY) and DATE("{{ ds() }}")
  {% endif %}
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13

)


SELECT
    *,
    ROUND(total_bytes_billed / POWER(2, 40) * 5, 2)  as cost_usd,
    ROUND(total_bytes_billed / POWER(2, 40) * 5, 2) / 1.08  as cost_euro,
FROM bq_costs
