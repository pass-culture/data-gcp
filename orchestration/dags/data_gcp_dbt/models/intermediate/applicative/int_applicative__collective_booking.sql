SELECT
    cb.collective_booking_id,
    cb.collective_booking_creation_date,
    cb.collective_booking_used_date,
    collective_stock_id,
    cb.venue_id,
    cb.offerer_id,
    cb.collective_booking_cancellation_date,
    cb.collective_booking_cancellation_limit_date,
    cb.collective_booking_cancellation_reason,
    cb.collective_booking_status,
    cb.collective_booking_reimbursement_date,
    cb.educational_institution_id,
    cb.educational_year_id,
    cb.collective_booking_confirmation_date,
    cb.collective_booking_confirmation_limit_date,
    cb.educational_redactor_id,
    rank() OVER (
            PARTITION BY cb.educational_institution_id
            ORDER BY
                cb.collective_booking_creation_date
        ) AS collective_booking_rank,
FROM {{ source('raw', 'applicative_database_collective_booking') }} AS cb
