SELECT
    collective_booking_id,
    DATE(collective_booking_creation_date) AS collective_booking_creation_date,
    collective_booking_creation_date AS collective_booking_created_at,
    collective_booking_used_date,
    collective_stock_id,
    venue_id,
    offerer_id,
    collective_booking_cancellation_date,
    collective_booking_cancellation_date IS NOT NULL AS collective_booking_is_cancelled,
    collective_booking_cancellation_limit_date,
    collective_booking_cancellation_reason,
    collective_booking_status,
    collective_booking_reimbursement_date,
    educational_institution_id,
    cb.educational_year_id,
    collective_booking_confirmation_date,
    collective_booking_confirmation_limit_date,
    educational_redactor_id,
    ey.educational_year_beginning_date,
    ey.educational_year_expiration_date,
    CURRENT_DATE BETWEEN ey.educational_year_beginning_date
        AND ey.educational_year_expiration_date AS is_current_educational_year,
    RANK() OVER (
            PARTITION BY educational_institution_id
            ORDER BY
                collective_booking_creation_date
        ) AS collective_booking_rank
FROM {{ source('raw', 'applicative_database_collective_booking') }} AS cb
LEFT JOIN {{ source('raw', 'applicative_database_educational_year') }} AS ey
    ON cb.educational_year_id = ey.educational_year_id
