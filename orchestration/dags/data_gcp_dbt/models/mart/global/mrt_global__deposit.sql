WITH bookings_grouped_by_deposit AS (
    SELECT deposit_id,
        MAX(user_civility) AS user_civility,
        MAX(user_department_code) AS user_department_code,
        MAX(user_age) AS user_age,
        MAX(user_is_active) AS user_is_active,
        MAX(user_creation_date) AS user_creation_date,
        MAX(user_birth_date) AS user_birth_date,
        SUM(CASE WHEN booking_is_used THEN booking_amount * booking_quantity END) AS total_actual_amount_spent,
        SUM(booking_amount * booking_quantity) AS total_theoretical_amount_spent,
        SUM(CASE WHEN digital_goods
            THEN booking_amount * booking_quantity END) AS total_theoretical_amount_spent_in_digital_goods,
        MIN(booking_creation_date) AS first_individual_booking_date,
        MAX(booking_creation_date) AS last_individual_booking_date,
        COUNT(DISTINCT booking_id) AS total_non_cancelled_individual_bookings
    FROM {{ ref('mrt_global__booking') }}
    WHERE NOT booking_is_cancelled
    GROUP BY deposit_id
)

SELECT
    d.deposit_id,
    d.deposit_amount,
    d.user_id,
    bgd.user_civility,
    bgd.user_department_code,
    bgd.user_age,
    region_department.region_name AS user_region_name,
    d.deposit_source,
    d.deposit_creation_date,
    d.deposit_update_date,
    d.deposit_expiration_date,
    d.deposit_type,
    d.deposit_rank_asc,
    d.deposit_rank_desc,
    bgd.total_theoretical_amount_spent,
    bgd.total_actual_amount_spent,
    bgd.total_theoretical_amount_spent_in_digital_goods,
    bgd.total_non_cancelled_individual_bookings,
    bgd.first_individual_booking_date,
    bgd.last_individual_booking_date,
    DATE_DIFF(
        CURRENT_DATE(),
        CAST(d.deposit_creation_date AS DATE),
        DAY
    ) AS deposit_seniority,
    DATE_DIFF(
        CAST(d.deposit_creation_date AS DATE),
        CAST(bgd.user_creation_date AS DATE),
        DAY
    ) AS days_between_user_creation_and_deposit_creation,
    bgd.user_birth_date
FROM {{ ref('int_applicative__deposit') }} AS d
LEFT JOIN bookings_grouped_by_deposit AS bgd ON bgd.deposit_id = d.deposit_id
LEFT JOIN {{ source('analytics','region_department') }} AS region_department ON bgd.user_department_code = region_department.num_dep
LEFT JOIN {{ ref('int_applicative__action_history') }} AS ah ON ah.user_id = d.user_id AND ah.action_history_rk = 1
WHERE
    (
        bgd.user_is_active
        OR ah.action_history_reason = 'upon user request'
    )