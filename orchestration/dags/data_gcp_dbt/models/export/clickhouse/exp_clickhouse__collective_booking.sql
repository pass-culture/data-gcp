select
    DATE("{{ ds() }}") as update_date,
    offerer_id,
    collective_offer_id,
    offer_id,
    DATE(collective_booking_creation_date) as creation_date,
    DATE(collective_booking_used_date) as used_date,
    DATE(collective_booking_reimbursement_date) as reimbursement_date,
    collective_booking_status,
    educational_institution_id,
    number_of_tickets,
    booking_amount
from {{ ref('enriched_collective_booking_data') }}
where collective_booking_status != 'CANCELLED'
