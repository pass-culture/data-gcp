-- Rank only: the revenue behind the ranking is not exported (no revenue at venue
-- level).
with
    tops as (
        select
            partition_month,
            geo_level,
            geo_value,
            venue_rank,
            venue_tag_name,
            venue_id,
            venue_name,
            offerer_name
        from {{ ref("metrics_top__venue_labelled") }}
    )

select
    tops.partition_month,
    tops.geo_level,
    geo.region_code,
    geo.region_name,
    geo.department_code,
    geo.department_name,
    tops.venue_rank,
    tops.venue_tag_name,
    tops.venue_id,
    tops.venue_name,
    tops.offerer_name
from tops {{ top_ranking_geo_join("tops") }}
