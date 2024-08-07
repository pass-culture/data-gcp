with venues as (
    select
        venue_id,
        venue_longitude,
        venue_latitude
    from {{ ref("mrt_global__venue") }}

),

offer_details as (
    select
        eod.item_id,
        eod.offer_id,
        eod.offer_name,
        v.venue_id,
        v.venue_longitude,
        v.venue_latitude
    from {{ ref('mrt_global__offer') }} eod
        left join venues v on v.venue_id = eod.venue_id
    qualify ROW_NUMBER() over (partition by eod.item_id order by eod.total_used_individual_bookings desc) = 1
),


recommendable_items_raw as (

    select
        ro.item_id,
        MAX(ro.topic_id) as topic_id,
        MAX(ro.cluster_id) as cluster_id,
        MAX(ro.category) as category,
        MAX(ro.subcategory_id) as subcategory_id,
        MAX(ro.search_group_name) as search_group_name,
        MAX(ro.is_numerical) as is_numerical,
        MAX(ro.is_national) as is_national,
        MAX(ro.is_geolocated) as is_geolocated,
        MAX(ro.offer_is_duo) as offer_is_duo,
        MAX(ro.offer_type_domain) as offer_type_domain,
        MAX(ro.offer_type_label) as offer_type_label,
        MAX(ro.gtl_id) as gtl_id,
        MAX(ro.gtl_l1) as gtl_l1,
        MAX(ro.gtl_l2) as gtl_l2,
        MAX(ro.gtl_l3) as gtl_l3,
        MAX(ro.gtl_l4) as gtl_l4,
        MAX(ro.booking_number) as booking_number,
        MAX(ro.booking_number_last_7_days) as booking_number_last_7_days,
        MAX(ro.booking_number_last_14_days) as booking_number_last_14_days,
        MAX(ro.booking_number_last_28_days) as booking_number_last_28_days,
        MAX(ro.is_underage_recommendable) as is_underage_recommendable,
        MAX(ro.is_sensitive) as is_sensitive,
        MAX(ro.is_restrained) as is_restrained,
        MIN(ro.offer_creation_date) as offer_creation_date,
        MIN(ro.stock_beginning_date) as stock_beginning_date,
        AVG(ro.stock_price) as stock_price,
        MAX(ro.total_offers) as total_offers,
        MAX(ro.semantic_emb_mean) as semantic_emb_mean

    from
        {{ ref('ml_reco__recommendable_offer') }} ro
    group by 1
),


trends as (
    select
        ro.*,
        COALESCE(booking_number_last_7_days * SAFE_DIVIDE((booking_number_last_7_days + booking_number_last_14_days), booking_number_last_28_days) * 0.5, 0) as booking_trend,
        LEAST(1, GREATEST(DATE_DIFF(CURRENT_DATE, COALESCE(stock_beginning_date, offer_creation_date), day), 1) / 60) as stock_date_penalty_factor,
        LEAST(1, GREATEST(DATE_DIFF(CURRENT_DATE, offer_creation_date, day), 1) / 60) as creation_date_penalty_factor,
        od.offer_name as example_offer_name,
        od.offer_id as example_offer_id,
        od.venue_id as example_venue_id,
        od.venue_longitude as example_venue_longitude,
        od.venue_latitude as example_venue_latitude
    from recommendable_items_raw ro
        left join offer_details od on od.item_id = ro.item_id
)

select
    *,
    SAFE_DIVIDE(booking_trend, stock_date_penalty_factor) as booking_release_trend,
    SAFE_DIVIDE(booking_trend, creation_date_penalty_factor) as booking_creation_trend
from trends
