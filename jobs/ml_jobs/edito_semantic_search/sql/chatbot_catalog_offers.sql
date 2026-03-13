-- CREATE OR REPLACE TABLE `{project}.{sandbox_dataset}.chatbot_edito_search_db_offers` AS
SELECT
    item_id,
    go.offer_id,
    go.offer_category_id,
    go.offer_subcategory_id,
    go.venue_department_code,
    go.last_stock_price,
    go.offer_creation_date,
    gs.stock_beginning_date
FROM `{project}.{analytics_dataset}.global_offer` go
JOIN `{project}.{analytics_dataset}.global_stock` gs
    ON go.offer_id = gs.offer_id
JOIN `{project}.{sandbox_dataset}.chatbot_edito_search_db_items` dbi
    ON dbi.id = go.item_id
WHERE
    (stock_beginning_date IS NULL OR stock_beginning_date > CURRENT_DATE())
    AND (
        go.offer_subcategory_id LIKE "%EVENEMENT%"
        OR go.offer_subcategory_id LIKE "%FESTIVAL%"
        OR go.offer_is_bookable
    )
    AND (
        go.total_individual_bookings > 1
        OR go.offer_subcategory_id <> 'LIVRE_PAPIER'
    )
GROUP BY
    item_id,
    offer_id,
    offer_category_id,
    offer_subcategory_id,
    go.venue_department_code,
    go.last_stock_price,
    go.offer_creation_date,
    gs.stock_beginning_date
