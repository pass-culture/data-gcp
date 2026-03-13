-- CREATE OR REPLACE TABLE
-- `{project}.{sandbox_dataset}.chatbot_edito_search_db_offers` AS
select
    item_id,
    go.offer_id,
    go.offer_category_id,
    go.offer_subcategory_id,
    go.venue_department_code,
    go.last_stock_price,
    go.offer_creation_date,
    gs.stock_beginning_date
from `{project}.{analytics_dataset}.global_offer` go
join `{project}.{analytics_dataset}.global_stock` gs on go.offer_id = gs.offer_id
join
    `{project}.{sandbox_dataset}.chatbot_edito_search_db_items` dbi
    on dbi.id = go.item_id
where
    (stock_beginning_date is null or stock_beginning_date > current_date())
    and (
        go.offer_subcategory_id like "%EVENEMENT%"
        or go.offer_subcategory_id like "%FESTIVAL%"
        or go.offer_is_bookable
    )
    and (go.total_individual_bookings > 1 or go.offer_subcategory_id <> 'LIVRE_PAPIER')
group by
    item_id,
    offer_id,
    offer_category_id,
    offer_subcategory_id,
    go.venue_department_code,
    go.last_stock_price,
    go.offer_creation_date,
    gs.stock_beginning_date
