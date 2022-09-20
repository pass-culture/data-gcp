WITH ranked_deposit_asc AS (
    SELECT
        deposit.userId,
        deposit.id AS deposit_id,
        ROW_NUMBER() OVER(
            PARTITION BY deposit.userId
            ORDER BY
                deposit.dateCreated,
                id
        ) AS deposit_rank_asc
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_deposit AS deposit
),

ranked_deposit_desc AS (
    SELECT
        deposit.userId,
        deposit.id AS deposit_id,
        ROW_NUMBER() OVER(
            PARTITION BY deposit.userId
            ORDER BY
                deposit.dateCreated DESC,
                id DESC
        ) AS deposit_rank_desc
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_deposit AS deposit
),
actual_amount_spent AS (
    SELECT
        individual_booking.deposit_id,
        COALESCE(
            SUM(
                booking.booking_amount * booking.booking_quantity
            ),
            0
        ) AS deposit_actual_amount_spent
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_individual_booking AS individual_booking
        JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking ON booking.individual_booking_id = individual_booking.individual_booking_id
        AND booking.booking_is_used IS TRUE
        AND booking.booking_is_cancelled IS FALSE
    GROUP BY
        individual_booking.deposit_id
),

theoretical_amount_spent AS (
    SELECT
        individual_booking.deposit_id,
        COALESCE(
            SUM(
                booking.booking_amount * booking.booking_quantity
            ),
            0
        ) AS deposit_theoretical_amount_spent
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_individual_booking AS individual_booking
        JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking ON booking.individual_booking_id = individual_booking.individual_booking_id
        AND booking.booking_is_cancelled IS FALSE
    GROUP BY
        individual_booking.deposit_id
),
theoretical_amount_spent_in_digital_goods AS (
    SELECT
        individual_booking.deposit_id,
        COALESCE(
            SUM(
                booking.booking_amount * booking.booking_quantity
            ),
            0
        ) AS deposit_theoretical_amount_spent_in_digital_goods
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_individual_booking AS individual_booking
        JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking ON booking.individual_booking_id = individual_booking.individual_booking_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock ON booking.stock_id = stock.stock_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON stock.offer_id = offer.offer_id
        INNER JOIN `{{ bigquery_analytics_dataset }}`.subcategories AS subcategories ON offer.offer_subcategoryId = subcategories.id
    WHERE
        subcategories.is_digital_deposit = true
        AND offer.offer_url IS NOT NULL
        AND booking.booking_is_cancelled IS FALSE
    GROUP BY
        individual_booking.deposit_id
),

first_booking_date AS (
    SELECT
        individual_booking.deposit_id,
        MIN(booking.booking_creation_date) AS deposit_first_booking_date,
        MAX(booking.booking_creation_date) AS deposit_last_booking_date,
        COUNT(DISTINCT booking.booking_id) AS deposit_no_cancelled_bookings
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_individual_booking AS individual_booking
        JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking ON booking.individual_booking_id = individual_booking.individual_booking_id
        AND booking.booking_is_cancelled IS FALSE
    GROUP BY
        individual_booking.deposit_id
),

user_suspension_history AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY userId
            ORDER BY
                CAST(id AS INTEGER) DESC
        ) AS rank
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_user_suspension
)

SELECT
    deposit.id AS deposit_id,
    deposit.amount AS deposit_amount,
    deposit.userId AS user_id,
    user.user_civility,
    user.user_department_code,
    user.user_age,
    region_department.region_name AS user_region_name,
    deposit.source AS deposit_source,
    user.user_creation_date AS user_creation_date,
    deposit.dateCreated AS deposit_creation_date,
    deposit.dateUpdated AS deposit_update_date,
    deposit.expirationDate AS deposit_expiration_date,
    deposit.type AS deposit_type,
    ranked_deposit_asc.deposit_rank_asc,
    ranked_deposit_desc.deposit_rank_desc,
    deposit_theoretical_amount_spent,
    deposit_actual_amount_spent,
    deposit_theoretical_amount_spent_in_digital_goods,
    deposit_no_cancelled_bookings,
    deposit_first_booking_date,
    deposit_last_booking_date,
    DATE_DIFF(
        CURRENT_DATE(),
        CAST(deposit.dateCreated AS DATE),
        DAY
    ) AS deposit_seniority,
    DATE_DIFF(
        CAST(deposit.dateCreated AS DATE),
        CAST(user.user_creation_date AS DATE),
        DAY
    ) AS days_between_user_creation_and_deposit_creation,
    user.user_birth_date
FROM
    `{{ bigquery_analytics_dataset }}`.applicative_database_deposit AS deposit
    JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_user AS user ON user.user_id = deposit.userId
    JOIN ranked_deposit_asc ON ranked_deposit_asc.deposit_id = deposit.id
    JOIN ranked_deposit_desc ON ranked_deposit_desc.deposit_id = deposit.id
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.region_department ON user.user_department_code = region_department.num_dep
    LEFT JOIN actual_amount_spent ON deposit.id = actual_amount_spent.deposit_id
    LEFT JOIN theoretical_amount_spent ON deposit.id = theoretical_amount_spent.deposit_id
    LEFT JOIN theoretical_amount_spent_in_digital_goods ON deposit.id = theoretical_amount_spent_in_digital_goods.deposit_id
    LEFT JOIN first_booking_date ON deposit.id = first_booking_date.deposit_id
    LEFT JOIN user_suspension_history ON user_suspension_history.userId = user.user_id
    and rank = 1
WHERE
    user_role IN ('UNDERAGE_BENEFICIARY', 'BENEFICIARY')
    AND (
        user.user_is_active
        OR user_suspension_history.reasonCode = 'UPON_USER_REQUEST'
    )