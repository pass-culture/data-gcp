WITH date_of_first_bookings AS (
    SELECT
        booking.user_id,
        MIN(booking.booking_creation_date) AS first_booking_date
    FROM

        {{ ref('booking')}} AS booking
        JOIN {{ source('raw','applicative_database_stock')}} AS stock ON stock.stock_id = booking.stock_id
        JOIN {{ ref('offer') }} AS offer ON offer.offer_id = stock.offer_id
    WHERE
        booking.booking_is_cancelled IS FALSE
    GROUP BY
        user_id
),
date_of_second_bookings_ranked_booking_data AS (
    SELECT
        booking.user_id,
        booking.booking_creation_date,
        rank() OVER (
            PARTITION BY booking.user_id
            ORDER BY
                booking.booking_creation_date ASC
        ) AS rank_booking
    FROM
        {{ ref('booking')}} AS booking
        JOIN {{ source('raw','applicative_database_stock')}} AS stock ON stock.stock_id = booking.stock_id
        JOIN {{ ref('offer') }} AS offer ON offer.offer_id = stock.offer_id
    WHERE booking.booking_is_cancelled IS FALSE
),
date_of_second_bookings AS (
    SELECT
        user_id,
        booking_creation_date AS second_booking_date
    FROM
        date_of_second_bookings_ranked_booking_data
    WHERE
        rank_booking = 2
),
date_of_bookings_on_third_product_tmp AS (
    SELECT
        booking.*,
        offer.offer_subcategoryId,
        offer.offer_name,
        offer.offer_id,
        rank() OVER (
            PARTITION BY booking.user_id,
            offer.offer_subcategoryId
            ORDER BY
                booking.booking_creation_date
        ) AS rank_booking_in_cat
    FROM
        {{ ref('booking')}} AS booking
        JOIN {{ source('raw','applicative_database_stock')}} AS stock ON booking.stock_id = stock.stock_id
        JOIN {{ ref('offer') }} AS offer ON offer.offer_id = stock.offer_id
    WHERE booking.booking_is_cancelled IS FALSE
),
date_of_bookings_on_third_product_ranked_data AS (
    SELECT
        *,
        rank() OVER (
            PARTITION BY user_id
            ORDER BY
                booking_creation_date
        ) AS rank_cat
    FROM
        date_of_bookings_on_third_product_tmp
    WHERE
        rank_booking_in_cat = 1
),
date_of_bookings_on_third_product AS (
    SELECT
        user_id,
        booking_creation_date AS booking_on_third_product_date
    FROM
        date_of_bookings_on_third_product_ranked_data
    WHERE
        rank_cat = 3
),
number_of_bookings AS (
    SELECT
        booking.user_id,
        COUNT(booking.booking_id) AS number_of_bookings
    FROM
        {{ ref('booking')}} AS booking
        JOIN {{ source('raw','applicative_database_stock')}} AS stock ON stock.stock_id = booking.stock_id
        JOIN {{ ref('offer') }} AS offer ON offer.offer_id = stock.offer_id
    GROUP BY
        user_id
),
number_of_non_cancelled_bookings AS (
    SELECT
        booking.user_id,
        COUNT(booking.booking_id) AS number_of_bookings
    FROM
        {{ ref('booking')}} AS booking
        JOIN {{ source('raw','applicative_database_stock')}} AS stock ON stock.stock_id = booking.stock_id
        JOIN {{ ref('offer') }} AS offer ON offer.offer_id = stock.offer_id
        AND NOT booking.booking_is_cancelled
    GROUP BY
        user_id
),
actual_amount_spent AS (
    SELECT
        user.user_id,
        COALESCE(
            SUM(
                booking.booking_intermediary_amount
            ),
            0
        ) AS actual_amount_spent
    FROM
        {{ ref('user_beneficiary') }} AS user
        LEFT JOIN {{ ref('booking') }} AS booking ON user.user_id = booking.user_id
        AND booking.booking_is_used IS TRUE
    GROUP BY
        user.user_id
),
theoretical_amount_spent AS (
    SELECT
        user.user_id,
        COALESCE(
            SUM(
                 booking.booking_intermediary_amount
            ),
            0
        ) AS theoretical_amount_spent
    FROM
        {{ ref('user_beneficiary') }} AS user
        LEFT JOIN {{ ref('booking') }} AS booking ON user.user_id = booking.user_id
        AND booking.booking_is_cancelled IS FALSE
    GROUP BY
        user.user_id
),
theoretical_amount_spent_in_digital_goods_eligible_booking AS (
    SELECT
        booking.user_id,
        booking.booking_amount,
        booking.booking_quantity,
        booking.booking_intermediary_amount
    FROM
        {{ ref('booking')}} AS booking
        LEFT JOIN {{ source('raw','applicative_database_stock')}} AS stock ON booking.stock_id = stock.stock_id
        LEFT JOIN {{ ref('offer') }} AS offer ON stock.offer_id = offer.offer_id
        INNER JOIN {{ source('clean','subcategories') }} AS subcategories ON offer.offer_subcategoryId = subcategories.id
    WHERE
        subcategories.is_digital_deposit = true
        AND offer.offer_url IS NOT NULL
        AND booking.booking_is_cancelled IS FALSE
),
theoretical_amount_spent_in_digital_goods AS (
    SELECT
        user.user_id,
        COALESCE(
            SUM(
                eligible_booking.booking_intermediary_amount
            ),
            0.
        ) AS amount_spent_in_digital_goods
    FROM
        {{ ref('user_beneficiary') }} AS user
        LEFT JOIN theoretical_amount_spent_in_digital_goods_eligible_booking eligible_booking ON user.user_id = eligible_booking.user_id
    GROUP BY
        user.user_id
),
theoretical_amount_spent_in_physical_goods_eligible_booking AS (
    SELECT
        booking.user_id,
        booking.booking_amount,
        booking.booking_quantity,
        booking.booking_intermediary_amount
    FROM
        {{ ref('booking')}} AS booking
        LEFT JOIN {{ source('raw','applicative_database_stock')}} AS stock ON booking.stock_id = stock.stock_id
        LEFT JOIN {{ ref('offer') }} AS offer ON stock.offer_id = offer.offer_id
        INNER JOIN {{ source('clean','subcategories') }} AS subcategories ON offer.offer_subcategoryId = subcategories.id
    WHERE
        subcategories.is_physical_deposit = true
        AND offer.offer_url IS NULL
        AND booking.booking_is_cancelled IS FALSE
),
theoretical_amount_spent_in_physical_goods AS (
    SELECT
        user.user_id,
        COALESCE(
            SUM(
                eligible_booking.booking_intermediary_amount
            ),
            0.
        ) AS amount_spent_in_physical_goods
    FROM
        {{ ref('user_beneficiary') }} AS user
        LEFT JOIN theoretical_amount_spent_in_physical_goods_eligible_booking eligible_booking ON user.user_id = eligible_booking.user_id
    GROUP BY
        user.user_id
),

theoretical_amount_spent_in_outings_eligible_booking AS (
    SELECT
        booking.user_id,
        booking.booking_amount,
        booking.booking_quantity,
        booking.booking_intermediary_amount
    FROM
        {{ ref('booking')}} AS booking
        LEFT JOIN {{ source('raw','applicative_database_stock')}} AS stock ON booking.stock_id = stock.stock_id
        LEFT JOIN {{ ref('offer') }} AS offer ON stock.offer_id = offer.offer_id
        INNER JOIN {{ source('clean','subcategories') }} AS subcategories ON offer.offer_subcategoryId = subcategories.id
    WHERE
        subcategories.is_event = true
        AND booking.booking_is_cancelled IS FALSE
),
theoretical_amount_spent_in_outings AS (
    SELECT
        user.user_id,
        COALESCE(
            SUM(
                eligible_booking.booking_intermediary_amount
            ),
            0.
        ) AS amount_spent_in_outings
    FROM
        {{ ref('user_beneficiary') }} AS user
        LEFT JOIN theoretical_amount_spent_in_outings_eligible_booking eligible_booking ON user.user_id = eligible_booking.user_id
    GROUP BY
        user.user_id
),
last_booking_date AS (
    SELECT
        booking.user_id,
        MAX(booking.booking_creation_date) AS last_booking_date
    FROM
        {{ ref('booking')}} AS booking
        JOIN {{ source('raw','applicative_database_stock')}} AS stock ON stock.stock_id = booking.stock_id
        JOIN {{ ref('offer') }} AS offer ON offer.offer_id = stock.offer_id
    GROUP BY
        user_id
),
first_paid_booking_date AS (
    SELECT
        booking.user_id,
        min(booking.booking_creation_date) AS booking_creation_date_first
    FROM
        {{ ref('booking')}} AS booking
        JOIN {{ source('raw','applicative_database_stock')}} AS stock ON stock.stock_id = booking.stock_id
        JOIN {{ ref('offer') }} AS offer ON offer.offer_id = stock.offer_id
        AND COALESCE(booking.booking_amount, 0) > 0
    GROUP BY
        user_id
),
first_booking_type_bookings_ranked AS (
    SELECT
        booking.booking_id,
        booking.user_id,
        offer.offer_subcategoryId,
        rank() over (
            partition by booking.user_id
            order by
                booking.booking_creation_date,
                booking.booking_id ASC
        ) AS rank_booking
    FROM
        {{ ref('booking')}} AS booking
        JOIN {{ source('raw','applicative_database_stock')}} AS stock ON booking.stock_id = stock.stock_id
        JOIN {{ ref('offer') }} AS offer ON offer.offer_id = stock.offer_id
),
first_booking_type AS (
    SELECT
        user_id,
        offer_subcategoryId AS first_booking_type
    FROM
        first_booking_type_bookings_ranked
    WHERE
        rank_booking = 1
),
first_paid_booking_type_paid_bookings_ranked AS (
    SELECT
        booking.booking_id,
        booking.user_id,
        offer.offer_subcategoryId,
        rank() over (
            partition by booking.user_id
            order by
                booking.booking_creation_date
        ) AS rank_booking
    FROM
        {{ ref('booking')}} AS booking
        JOIN {{ source('raw','applicative_database_stock')}} AS stock ON booking.stock_id = stock.stock_id
        JOIN {{ ref('offer') }} AS offer ON offer.offer_id = stock.offer_id
        AND booking.booking_amount > 0
),
first_paid_booking_type AS (
    SELECT
        user_id,
        offer_subcategoryId AS first_paid_booking_type
    FROM
        first_paid_booking_type_paid_bookings_ranked
    WHERE
        rank_booking = 1
),
count_distinct_types AS (
    SELECT
        booking.user_id,
        COUNT(DISTINCT offer.offer_subcategoryId) AS cnt_distinct_types
    FROM
        {{ ref('booking')}} AS booking
        JOIN {{ source('raw','applicative_database_stock')}} AS stock ON booking.stock_id = stock.stock_id
        JOIN {{ ref('offer') }} AS offer ON offer.offer_id = stock.offer_id
        AND NOT booking_is_cancelled
    GROUP BY
        user_id
),
user_agg_deposit_data_user_deposit_agg AS (
    SELECT
        userId,
        MIN(dateCreated) AS user_first_deposit_creation_date,
        MIN(amount) AS user_first_deposit_amount,
        MAX(amount) AS user_last_deposit_amount,
        MAX(expirationDate) AS user_last_deposit_expiration_date,
        SUM(amount) AS user_total_deposit_amount
    FROM
        {{ source('raw','applicative_database_deposit')}}
    GROUP BY
        1
),
user_agg_deposit_data AS (
    SELECT
        user_deposit_agg.*,
        CASE
            WHEN user_last_deposit_amount < 300 THEN 'GRANT_15_17'
            ELSE 'GRANT_18'
        END AS user_current_deposit_type,
        CASE
            WHEN user_first_deposit_amount < 300 THEN 'GRANT_15_17'
            ELSE 'GRANT_18'
        END AS user_first_deposit_type
    FROM
        user_agg_deposit_data_user_deposit_agg user_deposit_agg
),
last_deposit as (
    SELECT
        deposit.userId as user_id,
        deposit.id AS deposit_id,
    FROM
    {{ source('raw','applicative_database_deposit')}} AS deposit
    QUALIFY ROW_NUMBER() OVER(PARTITION BY deposit.userId ORDER BY deposit.dateCreated DESC, id DESC) = 1
),
amount_spent_last_deposit AS (
    SELECT
        booking.deposit_id
        , last_deposit.user_id
        , COALESCE(
            SUM(booking_intermediary_amount),
            0
        ) AS deposit_theoretical_amount_spent
        , SUM(CASE 
            WHEN booking_is_used IS TRUE
            THEN booking_intermediary_amount
            ELSE 0
        END) AS deposit_actual_amount_spent
        , SUM(CASE 
            WHEN subcategories.is_digital_deposit = true AND offer.offer_url IS NOT NULL
            THEN booking_intermediary_amount
            ELSE 0
        END) as deposit_theoretical_amount_spent_in_digital_goods
    FROM
        {{ ref('booking')}} AS booking
    JOIN last_deposit
        ON last_deposit.deposit_id = booking.deposit_id
    LEFT JOIN {{ source('raw','applicative_database_stock')}} AS stock 
        ON booking.stock_id = stock.stock_id
    LEFT JOIN {{ ref('offer') }} AS offer  
        ON stock.offer_id = offer.offer_id
    INNER JOIN {{ source('clean','subcategories') }} AS subcategories 
        ON offer.offer_subcategoryId = subcategories.id
    WHERE booking_is_cancelled IS FALSE
    GROUP BY
        deposit_id
        , last_deposit.user_id
)

SELECT
    user.user_id,
    user.user_department_code,
    user.user_postal_code,
    user.user_activity,
    user.user_civility,
    user.user_school_type,
    user.user_activation_date,
    user_agg_deposit_data.user_first_deposit_creation_date AS user_deposit_creation_date,
    user_agg_deposit_data.user_first_deposit_type AS user_first_deposit_type,
    user_agg_deposit_data.user_total_deposit_amount,
    user_agg_deposit_data.user_current_deposit_type,
    user.user_cultural_survey_filled_date AS first_connection_date,
    date_of_first_bookings.first_booking_date,
    date_of_second_bookings.second_booking_date,
    date_of_bookings_on_third_product.booking_on_third_product_date,
    COALESCE(number_of_bookings.number_of_bookings, 0) AS booking_cnt,
    COALESCE(
        number_of_non_cancelled_bookings.number_of_bookings,
        0
    ) AS no_cancelled_booking,
    DATE_DIFF(CURRENT_DATE(), CAST(user.user_activation_date AS DATE), DAY) AS user_seniority,
    actual_amount_spent.actual_amount_spent,
    theoretical_amount_spent.theoretical_amount_spent,
    theoretical_amount_spent_in_digital_goods.amount_spent_in_digital_goods,
    theoretical_amount_spent_in_physical_goods.amount_spent_in_physical_goods,
    theoretical_amount_spent_in_outings.amount_spent_in_outings,
    amount_spent_last_deposit.deposit_theoretical_amount_spent AS last_deposit_theoretical_amount_spent,
    amount_spent_last_deposit.deposit_theoretical_amount_spent_in_digital_goods AS last_deposit_theoretical_amount_spent_in_digital_goods,
    amount_spent_last_deposit.deposit_actual_amount_spent AS last_deposit_actual_amount_spent,
    user_last_deposit_amount,
    user_last_deposit_amount - amount_spent_last_deposit.deposit_theoretical_amount_spent AS user_theoretical_remaining_credit,
    user.user_humanized_id,
    last_booking_date.last_booking_date,
    region_department.region_name AS user_region_name,
    first_paid_booking_date.booking_creation_date_first,
    DATE_DIFF(
        date_of_first_bookings.first_booking_date,
        user_agg_deposit_data.user_first_deposit_creation_date,
        DAY
    ) AS days_between_activation_date_and_first_booking_date,
    DATE_DIFF(
        first_paid_booking_date.booking_creation_date_first,
        user_agg_deposit_data.user_first_deposit_creation_date,
        DAY
    ) AS days_between_activation_date_and_first_booking_paid,
    first_booking_type.first_booking_type,
    first_paid_booking_type.first_paid_booking_type,
    count_distinct_types.cnt_distinct_types AS cnt_distinct_type_booking,
    user.user_is_active,
    user_suspension.action_history_reason AS user_suspension_reason,
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
    user.user_age,
    user.user_birth_date,
    user.user_has_enabled_marketing_email,
FROM
    {{ ref('user_beneficiary') }} AS user
    LEFT JOIN date_of_first_bookings ON user.user_id = date_of_first_bookings.user_id
    LEFT JOIN date_of_second_bookings ON user.user_id = date_of_second_bookings.user_id
    LEFT JOIN date_of_bookings_on_third_product ON user.user_id = date_of_bookings_on_third_product.user_id
    LEFT JOIN number_of_bookings ON user.user_id = number_of_bookings.user_id
    LEFT JOIN number_of_non_cancelled_bookings ON user.user_id = number_of_non_cancelled_bookings.user_id
    LEFT JOIN actual_amount_spent ON user.user_id = actual_amount_spent.user_id
    LEFT JOIN theoretical_amount_spent ON user.user_id = theoretical_amount_spent.user_id
    LEFT JOIN theoretical_amount_spent_in_digital_goods ON user.user_id = theoretical_amount_spent_in_digital_goods.user_id
    LEFT JOIN theoretical_amount_spent_in_physical_goods ON user.user_id = theoretical_amount_spent_in_physical_goods.user_id
    LEFT JOIN theoretical_amount_spent_in_outings ON user.user_id = theoretical_amount_spent_in_outings.user_id
    LEFT JOIN last_booking_date ON last_booking_date.user_id = user.user_id
    LEFT JOIN {{source('analytics','region_department')}} ON user.user_department_code = region_department.num_dep
    LEFT JOIN first_paid_booking_date ON user.user_id = first_paid_booking_date.user_id
    LEFT JOIN first_booking_type ON user.user_id = first_booking_type.user_id
    LEFT JOIN first_paid_booking_type ON user.user_id = first_paid_booking_type.user_id
    LEFT JOIN count_distinct_types ON user.user_id = count_distinct_types.user_id
    LEFT JOIN {{ ref('user_suspension')}} AS user_suspension ON user_suspension.user_id = user.user_id
        AND rank = 1
    LEFT JOIN amount_spent_last_deposit ON amount_spent_last_deposit.user_id = user.user_id
    JOIN user_agg_deposit_data ON user.user_id = user_agg_deposit_data.userId
WHERE
    (
        user.user_is_active
        OR user_suspension.action_history_reason = 'upon user request'
    )

