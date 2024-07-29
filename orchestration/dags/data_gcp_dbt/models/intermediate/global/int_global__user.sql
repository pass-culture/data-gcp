{{
    config(
        materialized = "table"
    )
 }}

WITH bookings_deposit_grouped_by_user AS (
    SELECT b.user_id,
        COUNT(booking_id) AS total_individual_bookings,
        COUNT(CASE WHEN NOT booking_is_cancelled THEN booking_id END) AS total_non_cancelled_individual_bookings,
        SUM(CASE WHEN booking_is_used THEN booking_intermediary_amount END) AS total_actual_amount_spent,
        SUM(CASE WHEN NOT booking_is_cancelled THEN booking_intermediary_amount END) AS total_theoretical_amount_spent,
        MIN(CASE WHEN NOT booking_is_cancelled THEN booking_created_at END) AS first_individual_booking_date,
        MAX(booking_created_at) AS last_individual_booking_date,
        MIN(CASE WHEN booking_amount > 0 THEN booking_creation_date END) AS booking_creation_date_first,
        SUM(CASE WHEN physical_goods AND offer_url IS NULL AND NOT booking_is_cancelled THEN booking_intermediary_amount END) AS total_actual_amount_spent_in_physical_goods,
        SUM(CASE WHEN event
            AND NOT booking_is_cancelled THEN booking_intermediary_amount END) AS total_theoretical_amount_spent_in_outings,
        COUNT(DISTINCT CASE WHEN NOT booking_is_cancelled THEN offer_subcategory_id END) AS total_distinct_types,
        MIN(CASE WHEN user_booking_rank = 2 AND NOT booking_is_cancelled THEN booking_creation_date END) AS first_booking_date,
        MAX(CASE WHEN user_booking_id_rank = 1 THEN offer_subcategory_id END) AS  first_booking_type,
        MIN(CASE WHEN COALESCE(booking_amount, 0) > 0 THEN booking_creation_date END) AS first_paid_booking_date,
        MAX(CASE WHEN user_booking_rank = 2 AND NOT booking_is_cancelled THEN booking_creation_date END) AS second_booking_date,
        SUM(CASE WHEN deposit_rank_desc = 1 THEN booking_intermediary_amount END) AS deposit_theoretical_amount_spent,
        SUM(CASE WHEN NOT booking_is_cancelled
            AND deposit_rank_desc =1
            AND booking_is_used
            THEN booking_intermediary_amount END) AS deposit_actual_amount_spent,
        SUM(CASE WHEN deposit_rank_desc =1
            AND NOT booking_is_cancelled
            AND digital_goods = true AND offer_url IS NOT NULL
            THEN booking_intermediary_amount
            END) AS total_actual_amount_spent_in_digital_goods,
        SUM(CASE WHEN deposit_rank_desc =1
            AND digital_goods = true AND offer_url IS NOT NULL
            THEN booking_intermediary_amount
            END) AS total_theoretical_amount_spent_in_digital_goods,
        MAX(d.deposit_id) AS last_deposit_id,
        MAX(CASE
            WHEN offer_subcategory_id = 'ACTIVATION_THING'
            AND booking_used_date IS NOT NULL THEN booking_used_date
            ELSE NULL
        END) AS user_activation_date
    FROM {{ ref('int_global__booking')}} AS b
    LEFT JOIN {{ ref('int_applicative__deposit') }} AS d ON d.deposit_id = b.deposit_id
        AND deposit_rank_desc = 1
    GROUP BY user_id

),

deposit_grouped_by_user AS (
    SELECT user_id,
        MIN(deposit_creation_date) AS first_deposit_creation_date,
        MIN(deposit_amount) AS first_deposit_amount,
        MAX(deposit_amount) AS last_deposit_amount,
        MAX(deposit_expiration_date) AS last_deposit_expiration_date,
        SUM(deposit_amount) AS total_deposit_amount
    FROM
        {{ ref('int_applicative__deposit')}}
    GROUP BY user_id
),
user_agg_deposit_data AS (
    SELECT
        user_deposit_agg.*,
        CASE WHEN last_deposit_amount < 300 THEN 'GRANT_15_17'
            ELSE 'GRANT_18'
        END AS current_deposit_type,
        CASE
            WHEN first_deposit_amount < 300 THEN 'GRANT_15_17'
            ELSE 'GRANT_18'
        END AS first_deposit_type
    FROM
        deposit_grouped_by_user user_deposit_agg
),

ranked_for_bookings_not_canceled AS (

SELECT booking_id,
    user_id,
    booking_created_at,
    offer_subcategory_id,
    RANK() OVER (
        PARTITION BY user_id,
        offer_subcategory_id
        ORDER BY booking_created_at
    ) AS same_category_booking_rank,
    RANK() OVER (
        PARTITION BY user_id
        ORDER BY booking_created_at ASC
    ) AS user_booking_rank
FROM {{ ref('int_global__booking') }}
WHERE booking_is_cancelled IS FALSE

),

date_of_bookings_on_third_product AS (
    SELECT
        user_id,
        booking_created_at AS booking_on_third_product_date
    FROM ranked_for_bookings_not_canceled
    WHERE same_category_booking_rank = 1
    QUALIFY RANK() OVER (
            PARTITION BY user_id
            ORDER BY booking_created_at
        ) = 3
),


first_paid_booking_type AS (
    SELECT
        user_id,
        offer_subcategory_id AS first_paid_booking_type
    FROM {{ ref('mrt_global__booking') }}
    WHERE booking_amount > 0
    QUALIFY RANK() over (
            partition by user_id
            order by
                booking_created_at
        ) = 1
)


SELECT
    u.user_id,
    u.user_department_code,
    u.user_postal_code,
    u.user_activity,
    u.user_civility,
    u.user_school_type,
    u.user_cultural_survey_filled_date AS first_connection_date,
    u.user_is_active,
    u.user_age,
    u.user_birth_date,
    u.user_has_enabled_marketing_email,
    u.user_iris_internal_id,
    u.user_humanized_id,
    dgu.first_deposit_creation_date,
    ud.first_deposit_type,
    dgu.total_deposit_amount,
    ud.current_deposit_type,
    bdgu.first_booking_date,
    bdgu.second_booking_date,
    dbtp.booking_on_third_product_date,
    COALESCE(bdgu.total_individual_bookings, 0) AS total_individual_bookings,
    COALESCE( bdgu.total_non_cancelled_individual_bookings, 0) AS total_non_cancelled_individual_bookings,
    bdgu.total_actual_amount_spent,
    bdgu.total_theoretical_amount_spent,
    bdgu.total_actual_amount_spent_in_digital_goods,
    bdgu.total_actual_amount_spent_in_physical_goods,
    bdgu.total_theoretical_amount_spent_in_outings,
    bdgu.deposit_theoretical_amount_spent,
    bdgu.total_theoretical_amount_spent_in_digital_goods,
    bdgu.deposit_actual_amount_spent,
    dgu.last_deposit_amount,
    dgu.last_deposit_amount - bdgu.deposit_theoretical_amount_spent AS total_theoretical_remaining_credit,
    bdgu.last_individual_booking_date AS last_booking_date,
    bdgu.booking_creation_date_first,
    DATE_DIFF(bdgu.first_individual_booking_date,dgu.first_deposit_creation_date,DAY) AS days_between_activation_date_and_first_booking_date,
    DATE_DIFF(bdgu.booking_creation_date_first,dgu.first_deposit_creation_date,DAY) AS days_between_activation_date_and_first_booking_paid,
    COALESCE(user_activation_date,user_creation_date) AS user_activation_date,
    bdgu.first_booking_type,
    first_paid_booking_type.first_paid_booking_type,
    bdgu.total_distinct_types,
    ah.action_history_reason AS user_suspension_reason,
    dgu.first_deposit_amount AS user_deposit_initial_amount,
    dgu.last_deposit_expiration_date AS user_deposit_expiration_date,
    CASE WHEN ( TIMESTAMP(dgu.last_deposit_expiration_date ) >= CURRENT_TIMESTAMP()
            AND COALESCE(bdgu.deposit_actual_amount_spent,0) < dgu.last_deposit_amount )
        AND u.user_is_active THEN TRUE ELSE FALSE END AS user_is_current_beneficiary
FROM {{ ref('int_applicative__user') }} AS u
LEFT JOIN {{ ref('int_applicative__action_history')}} AS ah ON ah.user_id = u.user_id AND ah.action_history_rk = 1
INNER JOIN user_agg_deposit_data AS ud ON ud.user_id = u.user_id
LEFT JOIN deposit_grouped_by_user AS dgu ON dgu.user_id = u.user_id
LEFT JOIN bookings_deposit_grouped_by_user AS bdgu ON bdgu.user_id = u.user_id
LEFT JOIN date_of_bookings_on_third_product AS dbtp ON dbtp.user_id = u.user_id
LEFT JOIN first_paid_booking_type ON u.user_id = first_paid_booking_type.user_id
WHERE
    (
        user_is_active
        OR action_history_reason = 'upon user request'
    )
