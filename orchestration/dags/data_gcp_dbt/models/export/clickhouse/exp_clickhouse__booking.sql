select
    venue_id,
    offerer_id,
    offer_id,
    date(booking_creation_date) as creation_date,
    date(booking_used_date) as used_date,
    date(booking_reimbursement_date) as reimbursement_date,
    date(stock_beginning_date) as stock_beginning_date,
    booking_status,
    deposit_type,
    booking_quantity,
    booking_amount
from {{ ref("mrt_global__booking") }}
where offerer_id is not null and booking_status != 'CANCELLED'
