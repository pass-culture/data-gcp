WITH venues AS (
        SELECT 
            venue_id, 
            venue_longitude,
            venue_latitude
        FROM `{{ bigquery_clean_dataset }}.applicative_database_venue` as venue
        JOIN  `{{ bigquery_clean_dataset }}.applicative_database_offerer` as offerer ON venue_managing_offerer_id=offerer_id
        WHERE venue.venue_is_virtual is false
        AND offerer.offerer_validation_status ='VALIDATED'
)

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
    ro.item_id,
    ro.offer_id,
    ro.product_id,
    ro.category,
    ro.subcategory_id,
    ro.search_group_name,
    ro.venue_id,
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
    v.venue_latitude,
    v.venue_longitude,
    CASE
        WHEN subcategories.category_id = 'MUSIQUE_LIVE' THEN 150000
        WHEN subcategories.category_id = 'MUSIQUE_ENREGISTREE'  THEN 50000
        WHEN subcategories.category_id = 'SPECTACLE' THEN 150000
        WHEN subcategories.category_id = 'CINEMA' THEN 50000
        WHEN subcategories.category_id = 'LIVRE' THEN 50000
        ELSE 100000
    END as default_max_distance,
    ROW_NUMBER() over() as unique_id,
FROM
    recommendable_offers_data ro
INNER JOIN `{{ bigquery_clean_dataset }}`.subcategories subcategories ON ro.subcategory_id = subcategories.id
INNER JOIN venues v ON ro.venue_id =   v.venueId
WHERE stock_rank < 30 -- only next 30 events
AND booking_number > 0 -- at least one bookking in period