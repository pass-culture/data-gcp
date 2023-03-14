WITH activation_dates AS (
    WITH ranked_bookings AS (
        SELECT
            booking.user_id,
            offer.offer_subcategoryId,
            booking_used_date,
            booking_is_used,
            RANK() OVER (
                PARTITION BY booking.user_id
                ORDER BY
                    booking.booking_creation_date ASC,
                    booking.booking_id ASC
            ) AS rank_
        FROM
            `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking
            JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock ON booking.stock_id = stock.stock_id
            JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON stock.offer_id = offer.offer_id
    )
    SELECT
        user.user_id,
        CASE
            WHEN "offer_subcategoryId" = 'ACTIVATION_THING'
            AND booking_used_date IS NOT NULL THEN booking_used_date
            ELSE user_creation_date
        END AS user_activation_date
    FROM
        `{{ bigquery_clean_dataset }}`.user_beneficiary AS user
        LEFT JOIN ranked_bookings ON user.user_id = ranked_bookings.user_id
        AND rank_ = 1
),
date_of_first_bookings AS (
    SELECT
        booking.user_id,
        MIN(booking.booking_creation_date) AS first_booking_date
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking
        JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock ON stock.stock_id = booking.stock_id
        JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON offer.offer_id = stock.offer_id
        AND offer.offer_subcategoryId != 'ACTIVATION_THING'
        AND (
            offer.booking_email != 'jeux-concours@passculture.app'
            OR offer.booking_email IS NULL
        )
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
        `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking
        JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock ON stock.stock_id = booking.stock_id
        JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON offer.offer_id = stock.offer_id
    WHERE
        offer.offer_subcategoryId != 'ACTIVATION_THING'
        AND (
            offer.booking_email != 'jeux-concours@passculture.app'
            OR offer.booking_email IS NULL
        )
        AND booking.booking_is_cancelled IS FALSE
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
        `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking
        JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock ON booking.stock_id = stock.stock_id
        JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON offer.offer_id = stock.offer_id
    WHERE
        offer.offer_subcategoryId NOT IN ('ACTIVATION_THING', 'ACTIVATION_THING')
        AND (
            offer.booking_email != 'jeux-concours@passculture.app'
            OR offer.booking_email IS NULL
        )
        AND booking.booking_is_cancelled IS FALSE
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
        `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking
        JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock ON stock.stock_id = booking.stock_id
        JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON offer.offer_id = stock.offer_id
        AND offer.offer_subcategoryId != 'ACTIVATION_THING'
        AND (
            offer.booking_email != 'jeux-concours@passculture.app'
            OR offer.booking_email IS NULL
        )
    GROUP BY
        user_id
),
number_of_non_cancelled_bookings AS (
    SELECT
        booking.user_id,
        COUNT(booking.booking_id) AS number_of_bookings
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking
        JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock ON stock.stock_id = booking.stock_id
        JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON offer.offer_id = stock.offer_id
        AND offer.offer_subcategoryId != 'ACTIVATION_THING'
        AND (
            offer.booking_email != 'jeux-concours@passculture.app'
            OR offer.booking_email IS NULL
        )
        AND NOT booking.booking_is_cancelled
    GROUP BY
        user_id
),
users_seniority_validated_activation_booking AS (
    SELECT
        booking.booking_used_date,
        booking.user_id,
        booking.booking_is_used
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking
        JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock ON stock.stock_id = booking.stock_id
        JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON stock.offer_id = offer.offer_id
        AND offer.offer_subcategoryId = 'ACTIVATION_THING'
    WHERE
        booking.booking_is_used
),
users_seniority_activation_date AS (
    SELECT
        CASE
            WHEN validated_activation_booking.booking_is_used THEN validated_activation_booking.booking_used_date
            ELSE user.user_creation_date
        END AS activation_date,
        user.user_id
    FROM
        `{{ bigquery_clean_dataset }}`.user_beneficiary AS user
        LEFT JOIN users_seniority_validated_activation_booking validated_activation_booking ON validated_activation_booking.user_id = user.user_id
),
users_seniority AS (
    SELECT
        DATE_DIFF(
            CURRENT_DATE(),
            CAST(activation_date.activation_date AS DATE),
            DAY
        ) AS user_seniority,
        user.user_id
    FROM
        `{{ bigquery_clean_dataset }}`.user_beneficiary AS user
        LEFT JOIN users_seniority_activation_date activation_date ON user.user_id = activation_date.user_id
),
actual_amount_spent AS (
    SELECT
        user.user_id,
        COALESCE(
            SUM(
                booking.booking_amount * booking.booking_quantity
            ),
            0
        ) AS actual_amount_spent
    FROM
        `{{ bigquery_clean_dataset }}`.user_beneficiary AS user
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking ON user.user_id = booking.user_id
        AND booking.booking_is_used IS TRUE
        AND booking.booking_is_cancelled IS FALSE
    GROUP BY
        user.user_id
),
theoretical_amount_spent AS (
    SELECT
        user.user_id,
        COALESCE(
            SUM(
                booking.booking_amount * booking.booking_quantity
            ),
            0
        ) AS theoretical_amount_spent
    FROM
        `{{ bigquery_clean_dataset }}`.user_beneficiary AS user
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking ON user.user_id = booking.user_id
        AND booking.booking_is_cancelled IS FALSE
    GROUP BY
        user.user_id
),
theoretical_amount_spent_in_digital_goods_eligible_booking AS (
    SELECT
        booking.user_id,
        booking.booking_amount,
        booking.booking_quantity
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock ON booking.stock_id = stock.stock_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON stock.offer_id = offer.offer_id
        INNER JOIN `{{ bigquery_analytics_dataset }}`.subcategories AS subcategories ON offer.offer_subcategoryId = subcategories.id
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
                eligible_booking.booking_amount * eligible_booking.booking_quantity
            ),
            0.
        ) AS amount_spent_in_digital_goods
    FROM
        `{{ bigquery_clean_dataset }}`.user_beneficiary AS user
        LEFT JOIN theoretical_amount_spent_in_digital_goods_eligible_booking eligible_booking ON user.user_id = eligible_booking.user_id
    GROUP BY
        user.user_id
),
theoretical_amount_spent_in_physical_goods_eligible_booking AS (
    SELECT
        booking.user_id,
        booking.booking_amount,
        booking.booking_quantity
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock ON booking.stock_id = stock.stock_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON stock.offer_id = offer.offer_id
        INNER JOIN `{{ bigquery_analytics_dataset }}`.subcategories AS subcategories ON offer.offer_subcategoryId = subcategories.id
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
                eligible_booking.booking_amount * eligible_booking.booking_quantity
            ),
            0.
        ) AS amount_spent_in_physical_goods
    FROM
        `{{ bigquery_clean_dataset }}`.user_beneficiary AS user
        LEFT JOIN theoretical_amount_spent_in_physical_goods_eligible_booking eligible_booking ON user.user_id = eligible_booking.user_id
    GROUP BY
        user.user_id
),

theoretical_amount_spent_in_outings_eligible_booking AS (
    SELECT
        booking.user_id,
        booking.booking_amount,
        booking.booking_quantity
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock ON booking.stock_id = stock.stock_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON stock.offer_id = offer.offer_id
        INNER JOIN `{{ bigquery_analytics_dataset }}`.subcategories AS subcategories ON offer.offer_subcategoryId = subcategories.id
    WHERE
        subcategories.is_event = true
        AND booking.booking_is_cancelled IS FALSE
),
theoretical_amount_spent_in_outings AS (
    SELECT
        user.user_id,
        COALESCE(
            SUM(
                eligible_booking.booking_amount * eligible_booking.booking_quantity
            ),
            0.
        ) AS amount_spent_in_outings
    FROM
        `{{ bigquery_clean_dataset }}`.user_beneficiary AS user
        LEFT JOIN theoretical_amount_spent_in_outings_eligible_booking eligible_booking ON user.user_id = eligible_booking.user_id
    GROUP BY
        user.user_id
),
last_booking_date AS (
    SELECT
        booking.user_id,
        MAX(booking.booking_creation_date) AS last_booking_date
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking
        JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock ON stock.stock_id = booking.stock_id
        JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON offer.offer_id = stock.offer_id
        AND offer.offer_subcategoryId != 'ACTIVATION_THING'
        AND (
            offer.booking_email != 'jeux-concours@passculture.app'
            OR offer.booking_email IS NULL
        )
    GROUP BY
        user_id
),
first_paid_booking_date AS (
    SELECT
        booking.user_id,
        min(booking.booking_creation_date) AS booking_creation_date_first
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking
        JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock ON stock.stock_id = booking.stock_id
        JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON offer.offer_id = stock.offer_id
        AND offer.offer_subcategoryId != 'ACTIVATION_THING'
        AND (
            offer.booking_email != 'jeux-concours@passculture.app'
            OR offer.booking_email IS NULL
        )
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
        `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking
        JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock ON booking.stock_id = stock.stock_id
        JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON offer.offer_id = stock.offer_id
        AND offer.offer_subcategoryId NOT IN ('ACTIVATION_THING', 'ACTIVATION_EVENT')
        AND (
            offer.booking_email != 'jeux-concours@passculture.app'
            OR offer.booking_email IS NULL
        )
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
        `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking
        JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock ON booking.stock_id = stock.stock_id
        JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON offer.offer_id = stock.offer_id
        AND offer.offer_subcategoryId NOT IN ('ACTIVATION_THING', 'ACTIVATION_EVENT')
        AND (
            offer.booking_email != 'jeux-concours@passculture.app'
            OR offer.booking_email IS NULL
        )
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
        `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking
        JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock ON booking.stock_id = stock.stock_id
        JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON offer.offer_id = stock.offer_id
        AND offer.offer_subcategoryId NOT IN ('ACTIVATION_THING', 'ACTIVATION_EVENT')
        AND (
            offer.booking_email != 'jeux-concours@passculture.app'
            OR offer.booking_email IS NULL
        )
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
        `{{ bigquery_analytics_dataset }}`.applicative_database_deposit
    GROUP BY
        1
),
user_agg_deposit_data AS (
    SELECT
        user_deposit_agg.*,
        CASE
            WHEN user_last_deposit_amount < 300 THEN 'GRANT_15_17'
            ELSE 'GRANT_18'
        END AS user_current_deposit_type
    FROM
        user_agg_deposit_data_user_deposit_agg user_deposit_agg
)

SELECT
    user.user_id,
    user.user_department_code,
    user.user_postal_code,
    user.user_activity,
    user.user_civility,
    user.user_school_type,
    activation_dates.user_activation_date,
    user_agg_deposit_data.user_first_deposit_creation_date AS user_deposit_creation_date,
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
    users_seniority.user_seniority,
    actual_amount_spent.actual_amount_spent,
    theoretical_amount_spent.theoretical_amount_spent,
    theoretical_amount_spent_in_digital_goods.amount_spent_in_digital_goods,
    theoretical_amount_spent_in_physical_goods.amount_spent_in_physical_goods,
    theoretical_amount_spent_in_outings.amount_spent_in_outings,
    last_deposit.deposit_theoretical_amount_spent AS last_deposit_theoretical_amount_spent,
    last_deposit.deposit_theoretical_amount_spent_in_digital_goods AS last_deposit_theoretical_amount_spent_in_digital_goods,
    last_deposit.deposit_actual_amount_spent AS last_deposit_actual_amount_spent,
    user_last_deposit_amount - last_deposit.deposit_theoretical_amount_spent AS user_theoretical_remaining_credit,
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
    user_suspension_history.reasonCode AS user_suspension_reason,
    user_agg_deposit_data.user_first_deposit_amount AS user_deposit_initial_amount,
    user_agg_deposit_data.user_last_deposit_expiration_date AS user_deposit_expiration_date,
    CASE
        WHEN TIMESTAMP(
            user_agg_deposit_data.user_last_deposit_expiration_date
        ) < CURRENT_TIMESTAMP()
        OR actual_amount_spent.actual_amount_spent >= user_agg_deposit_data.user_total_deposit_amount THEN TRUE
        ELSE FALSE
    END AS user_is_former_beneficiary,
    CASE
        WHEN (
            TIMESTAMP(
                user_agg_deposit_data.user_last_deposit_expiration_date
            ) >= CURRENT_TIMESTAMP()
            AND actual_amount_spent.actual_amount_spent < user_agg_deposit_data.user_total_deposit_amount
        )
        AND user_is_active THEN TRUE
        ELSE FALSE
    END AS user_is_current_beneficiary,
    user.user_age,
    user.user_birth_date,
    user.user_has_enabled_marketing_email,
FROM
    `{{ bigquery_clean_dataset }}`.user_beneficiary AS user
    LEFT JOIN activation_dates ON user.user_id = activation_dates.user_id
    LEFT JOIN date_of_first_bookings ON user.user_id = date_of_first_bookings.user_id
    LEFT JOIN date_of_second_bookings ON user.user_id = date_of_second_bookings.user_id
    LEFT JOIN date_of_bookings_on_third_product ON user.user_id = date_of_bookings_on_third_product.user_id
    LEFT JOIN number_of_bookings ON user.user_id = number_of_bookings.user_id
    LEFT JOIN number_of_non_cancelled_bookings ON user.user_id = number_of_non_cancelled_bookings.user_id
    LEFT JOIN users_seniority ON user.user_id = users_seniority.user_id
    LEFT JOIN actual_amount_spent ON user.user_id = actual_amount_spent.user_id
    LEFT JOIN theoretical_amount_spent ON user.user_id = theoretical_amount_spent.user_id
    LEFT JOIN theoretical_amount_spent_in_digital_goods ON user.user_id = theoretical_amount_spent_in_digital_goods.user_id
    LEFT JOIN theoretical_amount_spent_in_physical_goods ON user.user_id = theoretical_amount_spent_in_physical_goods.user_id
    LEFT JOIN theoretical_amount_spent_in_outings ON user.user_id = theoretical_amount_spent_in_outings.user_id
    LEFT JOIN last_booking_date ON last_booking_date.user_id = user.user_id
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.region_department ON user.user_department_code = region_department.num_dep
    LEFT JOIN first_paid_booking_date ON user.user_id = first_paid_booking_date.user_id
    LEFT JOIN first_booking_type ON user.user_id = first_booking_type.user_id
    LEFT JOIN first_paid_booking_type ON user.user_id = first_paid_booking_type.user_id
    LEFT JOIN count_distinct_types ON user.user_id = count_distinct_types.user_id
    LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_user_suspension AS user_suspension ON user_suspension.userId = user.user_id
        AND rank = 1
    LEFT JOIN  `{{ bigquery_analytics_dataset }}`.enriched_deposit_data AS last_deposit ON last_deposit.user_id = user.user_id
        AND deposit_rank_desc = 1
    JOIN user_agg_deposit_data ON user.user_id = user_agg_deposit_data.userId
WHERE
    (
        user.user_is_active
        OR user_suspension.reasonCode = 'UPON_USER_REQUEST'
    )