with
    metabase_query as (
        select
            mqe.execution_date,
            mqe.dashboard_id,
            mqe.card_id,
            mqe.execution_id,
            mqe.executor_id as metabase_user_id,
            mqe.cache_hit,
            mqe.error,
            mqe.context,
            to_hex(mqe.hash) as metabase_hash,
            sum(mqe.running_time) as running_time,
            sum(mqe.result_rows) as result_rows
        from {{ source("raw", "metabase_query_execution") }} as mqe
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9
    )

select
    mqe.*,
    mu.user_direction,
    mu.user_team,
    mrc.card_name,
    mrc.created_at as card_creation_date,
    mrc.updated_at as card_update_date,
    mrc.card_collection_id,
    mrd.dashboard_name,
    mrc.query_type,
    mrc.dataset_query,
    row_number() over (
        partition by mqe.card_id order by mqe.execution_date desc
    ) as card_id_execution_rank
from metabase_query as mqe
inner join {{ source("raw", "metabase_report_card") }} as mrc on mqe.card_id = mrc.id
left join
    {{ source("raw", "metabase_report_dashboard") }} as mrd on mqe.dashboard_id = mrd.id
left join {{ ref("int_metabase__user") }} as mu on mqe.metabase_user_id = mu.user_id
