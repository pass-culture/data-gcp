{{
    config(
        materialized = "view"
    )
}}

WITH recommendable_offers_data AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY offer_id ORDER BY stock_price, stock_beginning_date ASC) as stock_rank,
    FROM (
        SELECT 
            item_id,
            offer_id,
            product_id,
            venue_id,
            venue_latitude,
            venue_longitude,
            offer_creation_date,
            stock_beginning_date,
            MAX(stock_price) as stock_price,
            MAX(category) as category,
            MAX(offer_type_domain) as offer_type_domain,
            MAX(offer_type_label) as offer_type_label,
            MAX(ARRAY_TO_STRING(offer_type_labels, ';')) as offer_type_labels,
            MAX(item_count) as total_offers,
            MAX(booking_number) as booking_number,
            MAX(booking_number_last_7_days) AS booking_number_last_7_days,
            MAX(booking_number_last_14_days) AS booking_number_last_14_days,
            MAX(booking_number_last_28_days) AS booking_number_last_28_days,
            MAX(is_underage_recommendable) as is_underage_recommendable,
            MAX(is_sensitive) as is_sensitive,
            MAX(is_restrained) as is_restrained,
            MAX(subcategory_id) as subcategory_id,
            MAX(search_group_name) as search_group_name,
            MAX(name) as name,
            MAX(gtl_id) AS gtl_id,
            MAX(gtl_l1) AS gtl_l1,
            MAX(gtl_l2) AS gtl_l2,
            MAX(gtl_l3) AS gtl_l3,
            MAX(gtl_l4) AS gtl_l4,
            MAX(topic_id) AS topic_id,
            MAX(cluster_id) AS cluster_id,
            MAX(semantic_emb_mean) AS semantic_emb_mean,
            MAX(is_national) as is_national,
            MIN(url IS NOT NULL) as is_numerical,
            MAX((url IS NULL AND NOT is_national)) as is_geolocated,
            MAX(offer_is_duo) as offer_is_duo,
            MAX(default_max_distance) as default_max_distance    
        FROM {{ ref('ml_reco__available_offer') }}
        GROUP BY 1,2,3,4,5,6,7,8
    )
)

SELECT
    ro.item_id,
    ro.offer_id,
    ro.product_id,
    ro.category,
    ro.subcategory_id,
    ro.search_group_name,
    ro.venue_id,
    ro.venue_latitude,
    ro.venue_longitude,
    ro.name,
    ro.gtl_id,
    ro.gtl_l1,
    ro.gtl_l2,
    ro.gtl_l3,
    ro.gtl_l4,
    ro.topic_id,
    ro.cluster_id,
    ro.semantic_emb_mean,
    ro.is_numerical,
    ro.is_national,
    ro.is_geolocated,
    ro.offer_creation_date,
    ro.stock_beginning_date,
    ro.stock_price,
    ro.offer_is_duo,
    ro.offer_type_domain,
    ro.offer_type_label,
    ro.offer_type_labels,
    ro.total_offers,
    ro.booking_number,
    ro.booking_number_last_7_days,
    ro.booking_number_last_14_days,
    ro.booking_number_last_28_days,
    ro.is_underage_recommendable,
    ro.is_sensitive,
    ro.is_restrained,
    ro.default_max_distance,
    ROW_NUMBER() over() as unique_id
FROM
    recommendable_offers_data ro
WHERE stock_rank < 30 -- only next 30 events