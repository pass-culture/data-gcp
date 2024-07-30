{{
    config(
        **custom_incremental_config(
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "start_date", "data_type": "date"},
        on_schema_change = "sync_all_columns"
    )
) }}

with table_references as (
    select
        q.project_id,
        q.job_id,
        STRING_AGG(CONCAT(referenced_table_unn.dataset_id, '.', referenced_table_unn.table_id), "," order by CONCAT(referenced_table_unn.dataset_id, '.', referenced_table_unn.table_id)) as referenced_tables

    from {{ target.project }}.{{ var('region_name') }}.information_schema.jobs_by_project q,
        UNNEST(referenced_tables) as referenced_table_unn

    group by 1, 2
),

bq_costs as (
    select
        DATE(start_time) as start_date,
        DATE(creation_time) as creation_date,
        queries.project_id,
        queries.job_id,
        user_email,
        cache_hit,
        destination_table.dataset_id as dataset_id,
        destination_table.table_id as table_id,
        tr.referenced_tables,
        statement_type,
        query,
        CAST(REGEXP_EXTRACT(query, r"Metabase:: userID: ([0-9]+).*") as INT) as metabase_user_id,
        REGEXP_EXTRACT(query, r"queryHash: ([a-z-0-9]+)\n") as metabase_hash,
        SUM(total_bytes_billed) as total_bytes_billed,
        SUM(total_bytes_processed) as total_bytes_processed,
        COUNT(*) as total_queries
    from `{{ target.project }}.{{ var('region_name') }}.INFORMATION_SCHEMA.JOBS_BY_PROJECT` as queries
        left join table_references tr on queries.project_id = tr.project_id and queries.job_id = tr.job_id
    {% if is_incremental() %}
        where DATE(creation_time) between DATE_SUB(DATE("{{ ds() }}"), interval 28 day) and DATE("{{ ds() }}")
    {% endif %}
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13

)


select
    *,
    6.8 * total_bytes_billed / POWER(2, 40) as cost_euro, -- price estimation for 1TB
    total_bytes_billed / POWER(10, 9) as total_gigabytes_billed
from bq_costs
