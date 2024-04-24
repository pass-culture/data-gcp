SELECT
    stock_modified_at_last_provider_date,
    stock_id,
    stock_modified_date,
    stock_price,
    stock_quantity,
    stock_booking_limit_date,
    offer_id,
    stock_is_soft_deleted,
    stock_beginning_date,
    stock_creation_date,
    DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) as partition_date
FROM `{{ bigquery_raw_dataset }}`.`applicative_database_stock`
