SELECT
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
    DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) as partition_date
FROM `{{ bigquery_clean_dataset }}`.`applicative_database_booking`
