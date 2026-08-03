with
    taxonomy as (
        select collection_id, tier, certified, in_scope
        from {{ source("raw", "metabase_collection_taxonomy") }}
    ),

    dashboard_views as (
        select
            dashboard_id,
            sum(
                case
                    when
                        date(execution_date)
                        > date_sub(current_date(), interval 3 month)
                    then total_views
                    else 0
                end
            ) as total_views_3_months
        from {{ ref("int_metabase__daily_dashboard_view") }}
        where dashboard_id is not null
        group by 1
    ),

    documented_dashboards as (
        select distinct dashboard_id
        from {{ source("int_metabase", "dashboard_documentation") }}
        where dashboard_id is not null
    ),

    dashboard_quality as (
        select
            rd.id as dashboard_id,
            coalesce(t.certified, false) as certified,
            case
                t.tier
                when 'key_dashboard'
                then 3
                when 'thematique'
                then 2
                when 'chantier'
                then 1
                else 0
            end as tier_rank,
            doc.dashboard_id is not null as doc_backed,
            coalesce(dv.total_views_3_months, 0) as total_views_3_months,
            coalesce(t.in_scope, false) and not rd.archived as in_scope,
            100 * cast(coalesce(t.certified, false) as int64)
            + 10 * cast(doc.dashboard_id is not null as int64)
            + case
                t.tier
                when 'key_dashboard'
                then 3
                when 'thematique'
                then 2
                when 'chantier'
                then 1
                else 0
            end as home_score
        from {{ source("raw", "metabase_report_dashboard") }} as rd
        left join taxonomy as t on rd.dashboard_collection_id = t.collection_id
        left join dashboard_views as dv on rd.id = dv.dashboard_id
        left join documented_dashboards as doc on rd.id = doc.dashboard_id
    ),

    card_dashboard_edges as (
        select distinct card_id, dashboard_id
        from {{ source("raw", "metabase_report_dashboard_card") }}
        where card_id is not null
    ),

    card_home_ranked as (
        select
            e.card_id,
            dq.dashboard_id as home_dashboard_id,
            dq.home_score,
            row_number() over (
                partition by e.card_id
                order by
                    dq.home_score desc,
                    dq.total_views_3_months desc,
                    dq.dashboard_id asc
            ) as home_rank
        from card_dashboard_edges as e
        inner join dashboard_quality as dq on e.dashboard_id = dq.dashboard_id
        where dq.in_scope
    ),

    card_home as (
        select card_id, home_dashboard_id, home_score
        from card_home_ranked
        where home_rank = 1
    ),

    cards as (
        select
            rc.id as card_id,
            lower(
                regexp_replace(normalize(rc.card_name, nfd), r'\p{M}', '')
            ) as metric_key,
            coalesce(t.certified, false) as card_certified,
            case
                t.tier
                when 'key_dashboard'
                then 3
                when 'thematique'
                then 2
                when 'chantier'
                then 1
                else 0
            end as card_tier_rank,
            coalesce(t.in_scope, false) as in_scope
        from {{ source("raw", "metabase_report_card") }} as rc
        left join taxonomy as t on rc.card_collection_id = t.collection_id
    ),

    in_scope_cards as (
        select
            c.card_id,
            c.metric_key,
            c.card_certified,
            c.card_tier_rank,
            ch.home_dashboard_id,
            coalesce(ch.home_score, -1) as home_score
        from cards as c
        left join card_home as ch on c.card_id = ch.card_id
        where c.in_scope
    ),

    ranked as (
        select
            *,
            row_number() over (
                partition by metric_key
                order by
                    home_score desc,
                    coalesce(home_dashboard_id, 2147483647),
                    card_id asc
            ) as canonical_rank,
            count(*) over (partition by metric_key) as group_size
        from in_scope_cards
    ),

    canonical as (
        select
            metric_key,
            card_id as canonical_card_id,
            home_dashboard_id as canonical_dashboard_id
        from ranked
        where canonical_rank = 1
    ),

    resolved as (
        select
            r.card_id,
            r.metric_key,
            r.card_certified,
            r.card_tier_rank,
            r.home_dashboard_id,
            r.group_size,
            c.canonical_card_id,
            c.canonical_dashboard_id,
            r.card_id = c.canonical_card_id as is_canonical,
            r.card_id != c.canonical_card_id as is_duplicate,
            r.group_size >= 2 as has_duplicates
        from ranked as r
        inner join canonical as c on r.metric_key = c.metric_key
    )

select
    r.card_id,
    r.metric_key,
    r.canonical_card_id,
    r.canonical_dashboard_id,
    r.is_canonical,
    r.is_duplicate,
    r.has_duplicates,
    r.group_size,
    round(
        0.45 * cast(r.card_certified or coalesce(dq.certified, false) as int64)
        + 0.30 * greatest(r.card_tier_rank, coalesce(dq.tier_rank, 0)) / 3
        + 0.15 * cast(coalesce(dq.doc_backed, false) as int64)
        + 0.10 * cast(r.canonical_dashboard_id is not null as int64),
        4
    ) as card_quality
from resolved as r
left join dashboard_quality as dq on r.canonical_dashboard_id = dq.dashboard_id
