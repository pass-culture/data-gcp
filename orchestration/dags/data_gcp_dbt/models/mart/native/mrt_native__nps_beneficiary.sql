WITH active_deposit_at_end_date AS (
    SELECT
        r.user_id,
        r.response_id,
        deposit_type,
        deposit_creation_date,
        ROW_NUMBER() OVER (PARTITION BY r.user_id, r.response_id ORDER BY deposit_creation_date DESC) AS rank
    FROM {{ ref('mrt_global__deposit') }} AS d
    INNER JOIN {{ ref('int_qualtrics__nps_beneficiary_answer') }} AS r ON d.user_id = r.user_id
        AND d.deposit_creation_date <= DATE(r.end_date)
),

total_bookings_at_end_date AS (
    SELECT
        r.user_id,
        r.response_id,
        booking_rank AS total_bookings,
        ROW_NUMBER() OVER (PARTITION BY r.user_id, r.response_id ORDER BY booking_creation_date DESC) AS rank
    FROM {{ ref('mrt_global__booking') }} AS b
    INNER JOIN {{ ref('int_qualtrics__nps_beneficiary_answer') }} AS r ON b.user_id = r.user_id
        AND b.booking_creation_date <= DATE(r.end_date)
)



SELECT
    r.end_date AS response_date,
    r.user_id,
    r.response_id,
    d.deposit_type,
    u.user_civility,
    u.user_region_name,
    u.user_activity,
    u.user_is_in_qpv,
    SAFE_CAST(r.answer AS INT64) AS rating,
    DATE_DIFF(r.end_date, u.user_activation_date, DAY) AS user_seniority
FROM {{ ref('int_qualtrics__nps_beneficiary_answer') }} AS r
INNER JOIN {{ ref('mrt_global__user') }} AS u ON r.user_id = u.user_id
LEFT JOIN active_deposit_at_end_date AS d ON r.user_id = d.user_id
    AND r.response_id = d.response_id
    AND d.rank = 1
LEFT JOIN total_bookings_at_end_date AS b ON r.user_id = b.user_id
    AND r.response_id = b.response_id
    AND b.rank = 1
WHERE r.is_nps_question = true
