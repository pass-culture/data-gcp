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
        ROW_NUMBER() OVER (PARTITION BY offer_id, ORDER BY stock_price, stock_beginning_date ASC) as stock_rank,
    FROM (
        SELECT 
            offer_id,
            offer_creation_date,
            DATE(stock_beginning_date) as stock_beginning_date,
            MAX(stock_price) as stock_price,
            MAX(category) as category,
            MAX(movie_type) as movie_type,
            MAX(offer_type_id) as offer_type_id,
            MAX(offer_type_label) as offer_type_label,
            MAX(offer_sub_type_id) as offer_sub_type_id,
            MAX(offer_sub_type_label) as offer_sub_type_label,
            MAX(macro_rayon) as macro_rayon,
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
        GROUP BY 1,2,3
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
        WHEN si.venue_distance_to_iris < 25 THEN "0_25" 
        WHEN si.venue_distance_to_iris < 50 THEN "20_50" 
        WHEN si.venue_distance_to_iris < 100 THEN "50_100" 
        WHEN si.venue_distance_to_iris < 150 THEN "100_150" 
    ELSE  "150+"
    END AS venue_distance_to_iris_bucket,
    ro.name,
    ro.is_numerical,
    ro.is_national,
    ro.is_geolocated,
    ro.offer_creation_date,
    ro.stock_beginning_date,
    ro.stock_price,
    ro.offer_is_duo,
    ro.movie_type,
    ro.offer_type_id,
    ro.offer_type_label,
    ro.offer_sub_type_id,
    ro.offer_sub_type_label,
    ro.macro_rayon,
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
        ro.iris_id   =   v.iris_id
    AND ro.venue_id =   v.venue_id
    
    
GROUP by
    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
--- max volume per iris
QUALIFY ROW_NUMBER() OVER (PARTITION BY iris_id ORDER BY si.venue_distance_to_iris) < 10000