with
    venues as (
        select venue_id, venue_longitude, venue_latitude
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
        from {{ ref("mrt_global__offer") }} as eod
        left join venues as v on eod.venue_id = v.venue_id
        qualify
            row_number() over (
                partition by eod.item_id
                order by eod.total_used_individual_bookings desc
            )
            = 1
    ),

    recommendable_items_raw as (

        select
            ro.item_id,
            max(ro.topic_id) as topic_id,
            max(ro.cluster_id) as cluster_id,
            max(ro.category) as category,
            max(ro.subcategory_id) as subcategory_id,
            max(ro.search_group_name) as search_group_name,
            max(ro.is_numerical) as is_numerical,
            max(ro.is_geolocated) as is_geolocated,
            max(ro.offer_is_duo) as offer_is_duo,
            max(ro.offer_type_domain) as offer_type_domain,
            max(ro.offer_type_label) as offer_type_label,
            max(ro.gtl_id) as gtl_id,
            max(ro.gtl_l1) as gtl_l1,
            max(ro.gtl_l2) as gtl_l2,
            max(ro.gtl_l3) as gtl_l3,
            max(ro.gtl_l4) as gtl_l4,
            max(ro.booking_number) as booking_number,
            max(ro.booking_number_last_7_days) as booking_number_last_7_days,
            max(ro.booking_number_last_14_days) as booking_number_last_14_days,
            max(ro.booking_number_last_28_days) as booking_number_last_28_days,
            max(ro.is_underage_recommendable) as is_underage_recommendable,
            max(ro.is_sensitive) as is_sensitive,
            max(ro.is_restrained) as is_restrained,
            min(ro.offer_creation_date) as offer_creation_date,
            min(ro.stock_beginning_date) as stock_beginning_date,
            avg(ro.stock_price) as stock_price,
            max(ro.total_offers) as total_offers,
            max(ro.semantic_emb_mean) as semantic_emb_mean

        from {{ ref("ml_reco__recommendable_offer") }} as ro
        group by 1
    ),

    trends as (
        select
            ro.*,
            od.offer_name as example_offer_name,
            od.offer_id as example_offer_id,
            od.venue_id as example_venue_id,
            od.venue_longitude as example_venue_longitude,
            od.venue_latitude as example_venue_latitude,
            coalesce(
                ro.booking_number_last_7_days * safe_divide(
                    (ro.booking_number_last_7_days + ro.booking_number_last_14_days),
                    ro.booking_number_last_28_days
                )
                * 0.5,
                0
            ) as booking_trend,
            least(
                1,
                greatest(
                    date_diff(
                        current_date,
                        coalesce(ro.stock_beginning_date, ro.offer_creation_date),
                        day
                    ),
                    1
                )
                / 60
            ) as stock_date_penalty_factor,
            least(
                1, greatest(date_diff(current_date, ro.offer_creation_date, day), 1) / 60
            ) as creation_date_penalty_factor
        from recommendable_items_raw as ro
        left join offer_details as od on ro.item_id = od.item_id
    )

select
    *,
    safe_divide(booking_trend, stock_date_penalty_factor) as booking_release_trend,
    safe_divide(booking_trend, creation_date_penalty_factor) as booking_creation_trend
from trends
