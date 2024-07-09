
{{
    config(
        materialized = "incremental",
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "start_date", "data_type": "date"},
        on_schema_change = "sync_all_columns"
    )
}}


WITH bq_costs AS (
  SELECT
    date(start_time) as start_date,
    date(creation_time) as creation_date,
    project_id,
    user_email,
    cache_hit,
    destination_table.dataset_id,
    destination_table.table_id,
    statement_type,
    query,
    sum(total_bytes_billed) as total_bytes, 
    count(*) as total_queries
  FROM `{{ project }}.region-europe-west1`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
  {% if is_incremental() %}
    WHERE date(creation_time) BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 3 DAY) and DATE("{{ ds() }}")
  {% endif %}
  GROUP BY 1, 2, 3, 4, 5, 6,7, 8, 9

),


SELECT 
    *, 
    regexp_extract(query, r"Metabase:: userID: ([0-9]+).*") as metabase_user_id,
    regexp_extract(query, r"queryHash: ([a-z-0-9]+)\n") as metabase_hash,
    total_bytes * 5 / power(1024, 4) as cost_usd
FROM bq_costs

