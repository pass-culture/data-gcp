WITH bookings_deposit_grouped_by_user AS (
    SELECT user_id,
        COUNT(booking_id) AS total_individual_bookings,
        COUNT(CASE WHEN NOT booking_is_cancelled THEN booking_id END) AS total_non_cancelled_individual_bookings,
        -- ces calculs on le faisait que sur les user bénéficiaires
        SUM(CASE WHEN booking_is_used THEN booking.booking_intermediary_amount END) AS total_actual_amount_spent,
        SUM(CASE WHEN NOT booking_is_cancelled THEN booking.booking_intermediary_amount END) AS total_theoretical_amount_spent,


        -- voir si on continue à filtrer sur booking_is_cancelled et dans int_applicative__stock ou si on le retire des deux
        MIN(CASE WHEN NOT booking_is_cancelled THEN booking_creation_date) AS first_individual_booking_date,
        -- pourquoi là on ne filtre pas sur booking_is_cancelled is FALSE
        MAX(booking_creation_date) AS last_individual_booking_date,

        -- trouver un autre nom pour cette colonne
        MIN(CASE WHEN COALESCE(booking_amount, 0) > 0 THEN booking_creation_date END) AS booking_creation_date_first,


        SUM(CASE WHEN is_digital_deposit AND offer_url IS NOT NULL
            AND NOT booking_is_cancelled THEN eligible_booking.booking_intermediary_amount END) AS total_amount_spent_in_digital_goods,
        SUM(is_physical_deposit
            AND offer_url IS NULL
            AND NOT booking_is_cancelled THEN booking_intermediary_amount END) AS total_amount_spent_in_physical_goods,
        SUM(CASE WHEN is_event
            AND NOT booking_is_cancelled THEN booking_intermediary_amount END) AS total_amount_spent_in_outings,
        COUNT(DISTINCT CASE WHEN NOT booking_is_cancelled THEN offer_subcategoryId) AS total_distinct_types,

        -- revoir ce nom de colonne
        MIN(CASE WHEN user_booking_rank = 2 AND NOT booking_is_cancelled THEN booking_creation_date) AS first_booking_date,
        MAX(CASE WHEN user_booking_id_rank = 1 THEN offer_subcategoryId END) AS  first_booking_type,

        MAX(CASE WHEN user_booking_rank = 2 AND NOT booking_is_cancelled THEN booking_creation_date) AS second_booking_date,

        -- je sais plus à quoi correspondait cette colonne
        MAX(CASE WHEN same_category_booking_rank = 1 AND NOT booking_is_cancelled),

        -- DEPOSIT
            SUM(CASE WHEN rown_deposit = = 1 THEN booking_intermediary_amount END)  AS deposit_theoretical_amount_spent,
        SUM(CASE WHEN NOT booking_is_cancelled
            AND rown_deposit = = 1
            AND booking_is_used
            THEN booking_intermediary_amount
            ELSE 0 END) AS deposit_actual_amount_spent,
        SUM(CASE WHEN rown_deposit = = 1
            AND NOT booking_is_cancelled
            AND subcategories.is_digital_deposit = true AND offer.offer_url IS NOT NULL
            THEN booking_intermediary_amount
            ELSE 0
            END) as deposit_theoretical_amount_spent_in_digital_goods,

        -- check le left join, maybe faire un outer join et un coalesce sur le user_id
        MAX(CASE WHEN rown_deposit = 1 id END) AS last_deposit_id,
        MIN(dateCreated) AS first_deposit_creation_date,
        MIN(amount) AS first_deposit_amount,
        MAX(amount) AS last_deposit_amount,
        MAX(expirationDate) AS last_deposit_expiration_date,
        SUM(amount) AS total_deposit_amount,


        MAX(user_current_deposit_type) AS user_current_deposit_type

    FROM {{ ref('mrt_global__booking')}}
    LEFT JOIN {{ ref('int_applicative__deposit') }} ON last_deposit.deposit_id = booking.deposit_id
        AND rown_deposit = = 1
    GROUP BY user_id

),

user_agg_deposit_data AS (
    SELECT
        user_deposit_agg.user_id,
        CASE WHEN last_deposit_amount < 300 THEN 'GRANT_15_17' ELSE 'GRANT_18' END AS user_current_deposit_type,
        CASE WHEN first_deposit_amount < 300 THEN 'GRANT_15_17' ELSE 'GRANT_18' END AS user_first_deposit_type
    FROM bookings_deposit_grouped_by_user user_deposit_agg
),

ranked_for_bookings_not_canceled AS (

SELECT * EXCEPT(same_category_booking_rank,user_booking_rank),
    RANK() OVER (
        PARTITION BY booking.user_id,
        offer.offer_subcategoryId
        ORDER BY booking.booking_creation_date
    ) AS same_category_booking_rank,
    RANK() OVER (
        PARTITION BY user_id
        ORDER BY booking_creation_date ASC
    ) AS user_booking_rank,
FROM {{ ref('mrt_global__booking') }}
WHERE booking_is_cancelled IS FALSE

),

date_of_bookings_on_third_product AS (
    SELECT
        user_id,
        booking_creation_date AS booking_on_third_product_date,
    FROM ranked_for_bookings_not_canceled
    WHERE same_category_booking_rank = 1
    QUALIFY RANK() OVER (
            PARTITION BY user_id
            ORDER BY booking_creation_date
        ) = 3
),


first_paid_booking_type AS (
    SELECT
        user_id,
        offer_subcategory_id AS first_paid_booking_type,
    FROM {{ ref('mrt_global__booking')
    WHERE booking_amount > 0
    QUALIFY RANK() over (
            partition by user_id
            order by
                booking_creation_date
        ) = 1
)


SELECT
    u.user_id,
    u.user_department_code,
    u.user_postal_code,
    u.user_activity,
    u.user_civility,
    u.user_school_type,
    u.user_activation_date,
    u.user_seniority,

    user_agg_deposit_data.user_first_deposit_creation_date AS user_deposit_creation_date,
    user_agg_deposit_data.user_first_deposit_type AS user_first_deposit_type,
    user_agg_deposit_data.user_total_deposit_amount,
    user_agg_deposit_data.user_current_deposit_type,



    bdgu.first_connection_date,
    bdgu.first_booking_date,
    bdgu.second_booking_date,
    dbtp.booking_on_third_product_date,
    COALESCE(bdgu.total_individual_bookings, 0) AS total_individual_bookings,
    COALESCE( bdgu.total_non_cancelled_individual_bookings, 0) AS total_non_cancelled_individual_bookings,
    bdgu.total_actual_amount_spent,
    bdgu.total_theoretical_amount_spent,
    bdgu.total_amount_spent_in_digital_goods,
    bdgu.total_amount_spent_in_physical_goods,
    bdgu.amount_spent_in_outings,
    bdgu.deposit_theoretical_amount_spent AS last_deposit_theoretical_amount_spent,
    bdgu.deposit_theoretical_amount_spent_in_digital_goods AS last_deposit_theoretical_amount_spent_in_digital_goods,
    bdgu.deposit_actual_amount_spent AS last_deposit_actual_amount_spent,
    bdgu.last_deposit_amount AS user_last_deposit_amount,
    bdgu.last_deposit_amount - bdgu.deposit_theoretical_amount_spent AS user_theoretical_remaining_credit,
    u.user_humanized_id,
    bdgu.last_individual_booking_date AS last_booking_date,
    bdgu.booking_creation_date_first,

    DATE_DIFF(
        bdgu.first_individual_booking_date,
        bdgu.first_deposit_creation_date,
        DAY
    ) AS days_between_activation_date_and_first_booking_date,
    DATE_DIFF(
        bdgu.booking_creation_date_first,
        bdgu.first_deposit_creation_date,
        DAY
    ) AS days_between_activation_date_and_first_booking_paid,



    first_booking_type.first_booking_type,
    first_paid_booking_type.first_paid_booking_type,
    count_distinct_types.cnt_distinct_types AS cnt_distinct_type_booking,
    u.user_is_active,
    us.action_history_reason AS user_suspension_reason,
    user_agg_deposit_data.user_first_deposit_amount AS user_deposit_initial_amount,
    user_agg_deposit_data.user_last_deposit_expiration_date AS user_deposit_expiration_date,
    CASE
        WHEN (
            TIMESTAMP(
                user_agg_deposit_data.user_last_deposit_expiration_date
            ) >= CURRENT_TIMESTAMP()
            AND COALESCE(amount_spent_last_deposit.deposit_actual_amount_spent,0) < user_agg_deposit_data.user_last_deposit_amount
        )
        AND user_is_active THEN TRUE
        ELSE FALSE
    END AS user_is_current_beneficiary,
    u.user_age,
    u.user_birth_date,
    u.user_has_enabled_marketing_email,
    u.user_iris_internal_id

FROM {{ ref('int_applicative__user') }} AS u
LEFT JOIN {{ ref('mrt_global__user_suspension')}} AS us ON us.user_id = u.user_id AND rank = 1

LEFT JOIN user_agg_deposit_data AS ud ON ud.user_id = u.user_id
LEFT JOIN bookings_deposit_grouped_by_user AS bdgu ON bdgu.user_id = u.user_id
LEFT JOIN date_of_bookings_on_third_product AS dbtp ON dbtp.user_id = u.user_id

LEFT JOIN date_of_second_bookings ON u.user_id = date_of_second_bookings.user_id
LEFT JOIN date_of_bookings_on_third_product ON u.user_id = date_of_bookings_on_third_product.user_id
LEFT JOIN theoretical_amount_spent_in_digital_goods ON u.user_id = theoretical_amount_spent_in_digital_goods.user_id
LEFT JOIN theoretical_amount_spent_in_physical_goods ON u.user_id = theoretical_amount_spent_in_physical_goods.user_id
LEFT JOIN theoretical_amount_spent_in_outings ON u.user_id = theoretical_amount_spent_in_outings.user_id
LEFT JOIN first_paid_booking_date ON u.user_id = first_paid_booking_date.user_id
LEFT JOIN first_booking_type ON u.user_id = first_booking_type.user_id
LEFT JOIN first_paid_booking_type ON u.user_id = first_paid_booking_type.user_id
LEFT JOIN count_distinct_types ON u.user_id = count_distinct_types.user_id

LEFT JOIN amount_spent_last_deposit ON amount_spent_last_deposit.user_id = u.user_id
JOIN user_agg_deposit_data ON u.user_id = user_agg_deposit_data.userId
WHERE is_beneficiary is TRUE
    (
        u.user_is_active
        OR us.action_history_reason = 'upon user request'
    )

