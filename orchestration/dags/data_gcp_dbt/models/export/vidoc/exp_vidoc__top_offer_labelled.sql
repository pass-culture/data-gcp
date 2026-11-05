-- Rank only: the revenue and the booked quantity behind the ranking are not
-- exported (no revenue at offer or venue level).
with
    tops as (
        select
            partition_month,
            geo_level,
            geo_value,
            offer_rank,
            venue_tag_name,
            item_id,
            offer_name,
            offer_category_id,
            offer_subcategory_id
        from {{ ref("metrics_top__offer_labelled") }}
    )

select
    tops.partition_month,
    tops.geo_level,
    geo.region_code,
    geo.region_name,
    geo.department_code,
    geo.department_name,
    tops.offer_rank,
    tops.venue_tag_name,
    tops.item_id,
    tops.offer_name,
    tops.offer_category_id,
    tops.offer_subcategory_id
from tops {{ top_ranking_geo_join("tops") }}
