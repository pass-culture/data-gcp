WITH offer_details AS (
    SELECT 
        item_id,
        offer_id, 
        offer_name,
    FROM `{{ bigquery_analytics_dataset }}.enriched_offer_data` 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY booking_confirm_cnt DESC) = 1
),

recommendable_items_raw AS (

    SELECT
        ro.item_id,
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
        MAX(ro.booking_number_last_7_days) AS booking_number_last_7_days,
        MAX(ro.booking_number_last_14_days) AS booking_number_last_14_days,
        MAX(ro.booking_number_last_28_days) AS booking_number_last_28_days,
        MAX(ro.is_underage_recommendable) as is_underage_recommendable,
        MAX(ro.is_sensitive) as is_sensitive,
        MIN(ro.offer_creation_date) as offer_creation_date,
        MIN(ro.stock_beginning_date) as stock_beginning_date,
        AVG(ro.stock_price) as stock_price
    FROM
    `{{ bigquery_analytics_dataset }}`.recommendable_offers_raw ro
    GROUP BY 1
)


SELECT 
    ro.*, 
    od.offer_name as example_offer_name, 
    od.offer_id as example_offer_id
FROM recommendable_items_raw ro
LEFT JOIN offer_details od on od.item_id = ro.item_id