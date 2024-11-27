select
    venue_id,
    offerer_id,
    collective_offer_id,
    offer_id,
    date(collective_booking_creation_date) as creation_date,
    date(collective_booking_used_date) as used_date,
    date(collective_booking_reimbursement_date) as reimbursement_date,
    date(collective_stock_beginning_date_time) as stock_beginning_date,
    collective_booking_status,
    educational_institution_id,
    collective_stock_number_of_tickets as number_of_tickets,
    booking_amount
from {{ ref("mrt_global__collective_booking") }}
where offerer_id is not null and collective_booking_status != 'CANCELLED'
