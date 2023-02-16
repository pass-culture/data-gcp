WITH selected_items AS (
    SELECT
        *,
        'in' as position
    FROM
        `{{ bigquery_analytics_dataset }}.top_items_in_iris_shape`
    UNION
    ALL
    SELECT
        *,
        'out' as position
    FROM
        `{{ bigquery_analytics_dataset }}.top_items_out_iris_shape`
    UNION
    ALL
    SELECT
        *,
        'none' as position
    FROM
        `{{ bigquery_analytics_dataset }}.top_items_not_geolocated`
),

recommendable_offers_data AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY offer_id ORDER BY stock_price, stock_beginning_date ASC) as stock_rank,
    FROM (
        SELECT 
            offer_id,
            product_id,
            offer_creation_date,
            DATE(stock_beginning_date) as stock_beginning_date,
            MAX(stock_price) as stock_price,
            MAX(category) as category,
            MAX(offer_type_domain) as offer_type_domain,
            MAX(offer_type_label) as offer_type_label,
            MAX(booking_number) as booking_number,
            MAX(is_underage_recommendable) as is_underage_recommendable,
            MAX(subcategory_id) as subcategory_id,
            MAX(search_group_name) as search_group_name,
            MAX(name) as name,
            MAX(is_national) as is_national,
            MIN(url IS NOT NULL) as is_numerical,
            MAX((url IS NULL AND NOT is_national)) as is_geolocated,
            MAX(offer_is_duo) as offer_is_duo
        
        FROM `{{ bigquery_analytics_dataset }}.recommendable_offers_data` 
        WHERE (stock_beginning_date > CURRENT_DATE) OR (stock_beginning_date IS NULL)
        GROUP BY 1,2,3,4
    )
)

SELECT
    si.item_id,
    si.offer_id,
    ro.product_id,
    ro.category,
    ro.subcategory_id,
    ro.search_group_name,
    si.iris_id,
    si.venue_id,
    si.venue_distance_to_iris,
    CASE 
        WHEN si.venue_distance_to_iris < 25000 THEN "0_25KM" 
        WHEN si.venue_distance_to_iris < 50000 THEN "25_50KM" 
        WHEN si.venue_distance_to_iris < 100000 THEN "50_100KM" 
        WHEN si.venue_distance_to_iris < 150000 THEN "100_150KM" 
    ELSE  "150KM+"
    END AS venue_distance_to_iris_bucket,
    ro.name,
    ro.is_numerical,
    ro.is_national,
    ro.is_geolocated,
    ro.offer_creation_date,
    ro.stock_beginning_date,
    ro.stock_price,
    ro.offer_is_duo,
    ro.offer_type_domain,
    ro.offer_type_label,
    ro.booking_number,
    ro.is_underage_recommendable,
    si.position,
    v.venue_latitude,
    v.venue_longitude,
    ROW_NUMBER() over() as unique_id
FROM
    selected_items si
INNER JOIN recommendable_offers_data ro ON ro.offer_id = si.offer_id AND stock_rank < 30 -- get distinct tables with distinct heuristics
LEFT JOIN
     `{{ bigquery_analytics_dataset }}.iris_venues_at_radius` v 
     ON
        si.iris_id   =  v.irisId
    AND si.venue_id =   v.venueId
    
    
GROUP by
    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29
--- max volume per iris
QUALIFY ROW_NUMBER() OVER (PARTITION BY iris_id ORDER BY si.venue_distance_to_iris) < 10000