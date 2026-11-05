select
    collective_booking_id,
    collective_booking_creation_date,
    collective_booking_used_date,
    collective_stock_id,
    venue_id,
    offerer_id,
    collective_booking_cancellation_date,
    collective_booking_cancellation_limit_date,
    collective_booking_cancellation_reason,
    collective_booking_status,
    collective_booking_reimbursement_date,
    educational_institution_id,
    educational_year_id,
    collective_booking_confirmation_date,
    collective_booking_confirmation_limit_date,
    educational_redactor_id,
    date_add(current_date(), interval -1 day) as partition_date
from `{{ bigquery_raw_dataset }}`.`applicative_database_collective_booking`
