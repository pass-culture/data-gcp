SELECT
    DATE("{{ ds() }}") as update_date,
    offerer_id,
    collective_offer_id,
    offer_id,
    date(collective_booking_creation_date) as creation_date,
    date(collective_booking_used_date) as used_date,
    date(collective_booking_reimbursement_date) as reimbursement_date,
    collective_booking_status,
    educational_institution_id,
    number_of_tickets,
    booking_amount
FROM {{ ref('enriched_collective_booking_data') }}
WHERE not cast(collective_booking_is_cancelled as BOOL) and collective_booking_status != 'CANCELLED'
