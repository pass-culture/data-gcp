SELECT
    collective_booking_id,
    DATE(collective_booking_creation_date) AS collective_booking_creation_date,
    collective_booking_creation_date AS collective_booking_created_at,
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
    rank() OVER (
            PARTITION BY educational_institution_id
            ORDER BY
                collective_booking_creation_date
        ) AS collective_booking_rank
FROM {{ source('raw', 'applicative_database_collective_booking') }}
