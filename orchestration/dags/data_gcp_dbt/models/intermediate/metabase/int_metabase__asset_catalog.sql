with
    taxonomy as (
        select collection_id, squad, tier, certified, in_scope, is_excluded
        from {{ source("raw", "metabase_collection_taxonomy") }}
    ),

    public_collections as (
        select
            collection_id,
            collection_name,
            location,
            concat(location, collection_id, '/') as full_path
        from {{ source("raw", "metabase_collection") }}
        where personal_owner_id is null
    ),

    collection_ancestor_ids as (
        select
            c.collection_id as leaf_collection_id,
            cast(ancestor_id as int64) as ancestor_collection_id,
            ord  -- noqa: RF01
        from
            public_collections as c,
            unnest(split(trim(c.full_path, '/'), '/')) as ancestor_id
        with
        offset as ord
        where ancestor_id != ''
    ),

    collection_ancestors as (
        select
            a.leaf_collection_id as collection_id,
            array_agg(
                a.ancestor_collection_id order by a.ord
            ) as ancestor_collection_ids,
            array_agg(
                lower(c.collection_name) order by a.ord
            ) as ancestor_collection_names
        from collection_ancestor_ids as a
        inner join public_collections as c on a.ancestor_collection_id = c.collection_id
        group by 1
    ),

    dashboard_usage as (
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
            ) as total_views_3_months,
            max(execution_date) as last_execution_date
        from {{ ref("int_metabase__daily_dashboard_view") }}
        where dashboard_id is not null
        group by 1
    ),

    dashboard_members as (
        select
            dc.dashboard_id,
            array_agg(rc.card_name ignore nulls order by rc.card_name) as member_cards
        from {{ source("raw", "metabase_report_dashboard_card") }} as dc
        inner join
            {{ source("raw", "metabase_report_card") }} as rc on dc.card_id = rc.id
        where dc.card_id is not null
        group by 1
    ),

    card_assets as (
        select  -- noqa: ST06
            'card' as asset_kind,
            rc.id as asset_id,
            rc.card_name as asset_name,
            rc.card_description as description,
            concat('/question/', cast(rc.id as string)) as url,
            rc.card_collection_id as collection_id,
            rc.query_type,
            false as is_archived,
            cast(null as string) as dashboard_description,
            cast(null as string) as dashboard_markdown,
            coalesce(aca.total_views_3_months, 0) as total_views_3_months,
            aca.last_execution_date
        from {{ source("raw", "metabase_report_card") }} as rc
        left join
            {{ ref("int_metabase__aggregated_card_activity") }} as aca
            on rc.id = aca.card_id
    ),

    dashboard_assets as (
        select  -- noqa: ST06
            'dashboard' as asset_kind,
            rd.id as asset_id,
            rd.dashboard_name as asset_name,
            rd.dashboard_description as description,
            concat('/dashboard/', cast(rd.id as string)) as url,
            rd.dashboard_collection_id as collection_id,
            cast(null as string) as query_type,
            rd.archived as is_archived,
            rd.dashboard_description,
            dmd.dashboard_markdown,
            coalesce(du.total_views_3_months, 0) as total_views_3_months,
            du.last_execution_date
        from {{ source("raw", "metabase_report_dashboard") }} as rd
        left join dashboard_usage as du on rd.id = du.dashboard_id
        left join
            {{ ref("int_metabase__dashboard_markdown") }} as dmd
            on rd.id = dmd.dashboard_id
    ),

    assets as (
        select *
        from card_assets
        union all
        select *
        from dashboard_assets
    )

select
    a.asset_kind,
    a.asset_id,
    a.asset_name,
    a.description,
    a.url,
    a.query_type,
    a.collection_id,
    pc.collection_name,
    t.squad,
    t.tier,
    a.dashboard_description,
    a.dashboard_markdown,
    a.total_views_3_months,
    a.last_execution_date,
    a.is_archived,
    cc.metric_key,
    cc.canonical_card_id,
    cc.canonical_dashboard_id,
    cc.is_canonical,
    cc.card_quality,
    coalesce(anc.ancestor_collection_ids, []) as ancestor_collection_ids,
    coalesce(anc.ancestor_collection_names, []) as ancestor_collection_names,
    coalesce(t.certified, false) as certified,
    coalesce(t.in_scope, false) and not a.is_archived as in_scope,
    coalesce(t.is_excluded, true) or a.is_archived as is_excluded,
    coalesce(dmem.member_cards, []) as member_cards,
    coalesce(dp.dashboard_parameters, []) as dashboard_parameters,
    a.total_views_3_months > 0 as is_active,
    coalesce(cc.is_duplicate, false) as is_duplicate
from assets as a
left join taxonomy as t on a.collection_id = t.collection_id
left join public_collections as pc on a.collection_id = pc.collection_id
left join collection_ancestors as anc on a.collection_id = anc.collection_id
left join
    dashboard_members as dmem
    on a.asset_kind = 'dashboard'
    and a.asset_id = dmem.dashboard_id
left join
    {{ ref("int_metabase__dashboard_parameters") }} as dp
    on a.asset_kind = 'dashboard'
    and a.asset_id = dp.dashboard_id
left join
    {{ ref("int_metabase__card_canonical") }} as cc
    on a.asset_kind = 'card'
    and a.asset_id = cc.card_id
