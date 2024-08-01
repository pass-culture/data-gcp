select
    gbc.start_date,
    gbc.creation_date,
    coalesce(mu.email, gbc.user_email) as user_email,
    case
        when mu.email is not null then 'Metabase'
        when gbc.user_email like '%matabase%' then 'Metabase'
        when gbc.user_email like '%composer%' then 'Composer'
        else 'Bigquery (adhoc)'
    end
        as origin,
    gbc.dataset_id,
    gbc.table_id,
    mc.card_name,
    mc.dashboard_name,
    mc.card_id,
    mc.card_id_last_editor_email,
    mc.card_id_last_edition_date,
    mc.dashboard_id,
    sum(gbc.cost_euro) as cost_euro,
    sum(gbc.total_gigabytes_billed) as total_gigabytes,
    sum(gbc.total_bytes_billed) as total_bytes,
    sum(gbc.total_queries) as total_queries
from {{ ref('int_gcp__bigquery_cost') }} gbc
    left join {{ ref('int_metabase__user') }} mu on mu.user_id = gbc.metabase_user_id
    left join {{ ref('mrt_monitoring__metabase_cost') }} mc on
        (
            date(mc.date) = date(gbc.creation_date)
            and mc.metabase_hash = gbc.metabase_hash
            and mc.metabase_user_id = gbc.metabase_user_id
        )

where not cache_hit
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
