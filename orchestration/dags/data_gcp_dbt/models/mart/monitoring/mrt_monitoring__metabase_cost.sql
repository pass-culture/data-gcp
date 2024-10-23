with
    metabase_cost as (
        select
            creation_date,
            metabase_hash,
            metabase_user_id,
            sum(total_bytes_billed) as total_bytes_billed,
            sum(total_queries) as total_queries,
            sum(cost_euro) as cost_euro
        from {{ ref("int_gcp__bigquery_cost") }}
        group by creation_date, metabase_hash, metabase_user_id

    )

select
    date(mq.execution_date) as date,
    mq.metabase_hash,
    mq.dashboard_id,
    mq.card_id,
    aca.last_editor_email as card_id_last_editor_email,
    aca.last_edition_date as card_id_last_edition_date,
    mq.metabase_user_id,
    mq.card_name,
    mq.dashboard_name,
    avg(result_rows) as mean_result_rows,
    sum(total_bytes_billed) as total_bytes,
    sum(total_queries) as total_queries,
    sum(cost_euro) as cost_euro
from {{ ref("int_metabase__daily_query") }} mq
left join
    metabase_cost mc
    on date(mc.creation_date) = date(mq.execution_date)
    and mc.metabase_hash = mq.metabase_hash
    and mc.metabase_user_id = mq.metabase_user_id
left join
    {{ ref("int_metabase__aggregated_card_activity") }} aca on aca.card_id = mq.card_id
where not cache_hit  -- only thoses where we have real SQL queries
group by
    date(mq.execution_date),
    mq.metabase_hash,
    mq.dashboard_id,
    mq.card_id,
    aca.last_editor_email,
    aca.last_edition_date,
    mq.metabase_user_id,
    mq.card_name,
    mq.dashboard_name
