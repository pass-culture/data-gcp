select
    booking_id,
    booking_creation_date,
    stock_id,
    booking_quantity,
    user_id,
    booking_amount,
    booking_status,
    booking_is_cancelled,
    booking_is_used,
    booking_used_date,
    booking_cancellation_date,
    booking_cancellation_reason,
    booking_reimbursement_date,
    date_add(current_date(), interval -1 day) as partition_date
from `{{ bigquery_raw_dataset }}`.`applicative_database_booking`
