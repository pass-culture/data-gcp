select
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
    date_add(current_date(), interval -1 day) as partition_date
from `{{ bigquery_raw_dataset }}`.`applicative_database_stock`
