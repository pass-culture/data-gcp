with
    metabase_view as (
        select
            vl.id as view_id,
            vl.timestamp as view_date,
            vl.model_id as card_id,
            vl.user_id as metabase_user_id
        from {{ source("raw", "metabase_view_log") }} as vl
        where lower(vl.model) = 'card'
    )

select
    mv.view_id,
    mv.view_date,
    mv.card_id,
    mv.metabase_user_id,
    mu.user_direction,
    mu.user_team,
    mrc.card_name,
    mrc.created_at as card_creation_date,
    mrc.updated_at as card_update_date,
    mrc.card_collection_id,
    mrc.query_type,
    row_number() over (
        partition by mv.card_id order by mv.view_date desc
    ) as card_id_view_rank
from metabase_view as mv
inner join {{ source("raw", "metabase_report_card") }} as mrc on mv.card_id = mrc.id
left join {{ ref("int_metabase__user") }} as mu on mv.metabase_user_id = mu.user_id
