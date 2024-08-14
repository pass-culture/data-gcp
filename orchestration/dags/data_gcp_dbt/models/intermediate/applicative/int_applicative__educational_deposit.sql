SELECT
    ed.educational_institution_id AS institution_id,
    ed.educational_deposit_creation_date AS deposit_creation_date,
    ed.educational_deposit_amount,
    ey.educational_year_beginning_date AS educational_year_beginning_date,
    ey.educational_year_expiration_date AS educational_year_expiration_date,
    ed.ministry,
    CASE WHEN (
            CAST(ey.educational_year_beginning_date AS DATE) <= CURRENT_DATE
            AND CAST(ey.educational_year_expiration_date AS DATE) >= CURRENT_DATE
        ) THEN TRUE
        ELSE FALSE END AS is_current_deposit,
    RANK() OVER(
        PARTITION BY ed.educational_institution_id
        ORDER BY
            ed.educational_deposit_creation_date,
            ed.educational_deposit_id
    ) AS deposit_rank_asc,
    RANK() OVER(
        PARTITION BY ed.educational_institution_id
        ORDER BY
            ed.educational_deposit_creation_date DESC,
            ed.educational_deposit_id DESC
    ) AS deposit_rank_desc,
    RANK() OVER(
            PARTITION BY ed.educational_institution_id
            ORDER BY
                cb.collective_booking_creation_date
        ) AS booking_rank_asc,
    RANK() OVER(
        PARTITION BY ed.educational_institution_id
        ORDER BY
            cb.collective_booking_creation_date DESC
    ) AS booking_rank_desc,
    CASE
        WHEN (
            CAST(ey.educational_year_beginning_date AS DATE) <= CURRENT_DATE
            AND CAST(ey.educational_year_expiration_date AS DATE) >= CURRENT_DATE
        ) THEN TRUE
        ELSE FALSE
    END AS is_current_year_booking
FROM {{ source("raw", "applicative_database_educational_deposit") }} AS ed
LEFT JOIN {{ source("raw", "applicative_database_educational_year") }} AS ey ON ed.educational_year_id = ey.adage_id
LEFT JOIN {{ ref('int_applicative__collective_booking') }} AS cb ON ed.educational_institution_id = cb.educational_institution_id
    AND collective_booking_status != 'CANCELLED'
