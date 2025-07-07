select
    collective_stock_id,
    collective_stock_creation_date,
    collective_stock_modification_date,
    collective_stock_beginning_date_time,
    collective_offer_id,
    collective_stock_price,
    collective_stock_booking_limit_date_time,
    collective_stock_number_of_tickets,
    collective_stock_price_detail,
    date_add(current_date(), interval -1 day) as partition_date
from `{{ bigquery_raw_dataset }}`.`applicative_database_collective_stock`
