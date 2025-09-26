with
    metabase_query as (
        select
            mqe.execution_date,
            dashboard_id,
            mqe.card_id,
            mqe.execution_id,
            mqe.executor_id as metabase_user_id,
            mqe.cache_hit,
            mqe.error,
            mqe.context,
            to_hex(`hash`) as metabase_hash,
            sum(mqe.running_time) as running_time,
            sum(mqe.result_rows) as result_rows
        from {{ source("raw", "metabase_query_execution") }} as mqe
        group by
            execution_date,
            metabase_hash,
            dashboard_id,
            card_id,
            execution_id,
            metabase_user_id,
            cache_hit,
            error,
            context
    )

select
    mqe.*,
    mrc.card_name,
    mrc.created_at as card_creation_date,
    mrc.updated_at as card_update_date,
    mrc.card_collection_id,
    mrd.dashboard_name,
    mrc.query_type,
    mrc.dataset_query,
    row_number() over (
        partition by card_id order by execution_date desc
    ) as card_id_execution_rank
from metabase_query as mqe
inner join {{ source("raw", "metabase_report_card") }} as mrc on mqe.card_id = mrc.id
left join
    {{ source("raw", "metabase_report_dashboard") }} as mrd on mqe.dashboard_id = mrd.id
