{% set usage_safety_net_size = 100 %}

with
    catalog as (select * from {{ ref("int_metabase__asset_catalog") }}),

    eligible as (
        select *, (tier is not null or certified) as is_high_quality
        from catalog
        where in_scope and not is_excluded and is_active
    ),

    ranked as (
        select *, row_number() over (order by total_views_3_months desc) as usage_rank
        from eligible
    )

select
    asset_kind,
    asset_id,
    asset_name,
    collection_id,
    collection_name,
    squad,
    tier,
    certified,
    is_active,
    total_views_3_months,
    case
        when is_high_quality then 'classified' else 'usage_safety_net'
    end as selection_reason
from ranked
where is_high_quality or usage_rank <= {{ usage_safety_net_size }}
order by (asset_kind = 'dashboard') desc, certified desc, total_views_3_months desc
