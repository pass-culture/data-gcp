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
    MAX(ro.booking_number) as booking_number,
    MAX(ro.is_underage_recommendable) as is_underage_recommendable,
    MIN(ro.offer_creation_date) as offer_creation_date,
    MIN(ro.stock_beginning_date) as stock_beginning_date,
    AVG(ro.stock_price) as stock_price
FROM
   `{{ bigquery_analytics_dataset }}`.recommendable_offers_raw ro
GROUP BY 1