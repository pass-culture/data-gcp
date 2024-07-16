WITH bookings_grouped_by_deposit AS (
    SELECT deposit_id,
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
    u.user_civility,
    u.user_region_name,
    u.user_postal_code,
    u.user_city,
    u.user_epci,
    u.user_academy_name,
    u.user_density_label,
    u.user_macro_density_label,
    u.user_department_code,
    u.user_age,
    u.user_creation_date,
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
        CAST(u.user_creation_date AS DATE),
        DAY
    ) AS days_between_user_creation_and_deposit_creation,
    u.user_birth_date
FROM {{ ref('int_applicative__deposit') }} AS d
LEFT JOIN bookings_grouped_by_deposit AS bgd ON bgd.deposit_id = d.deposit_id
LEFT JOIN {{ ref('int_applicative__user') }} AS u ON u.user_id = d.user_id
LEFT JOIN {{ ref('int_applicative__action_history') }} AS ah ON ah.user_id = d.user_id AND ah.action_history_rk = 1
WHERE
    (
        u.user_is_active
        OR ah.action_history_reason = 'upon user request'
    )
