WITH ranked_deposit AS (
    SELECT
        deposit.userId,
        deposit.id AS deposit_id,
        RANK() OVER(
            PARTITION BY deposit.userId
            ORDER BY
                deposit.dateCreated,
                id
        ) AS deposit_rank
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
    region_department.region_name AS user_region_name,
    deposit.source AS deposit_source,
    user.user_creation_date AS user_creation_date,
    deposit.dateCreated AS deposit_creation_date,
    deposit.dateUpdated AS deposit_update_date,
    deposit.expirationDate AS deposit_expiration_date,
    deposit.type AS deposit_type,
    ranked_deposit.deposit_rank,
    deposit_theoretical_amount_spent,
    deposit_actual_amount_spent,
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
    JOIN ranked_deposit ON ranked_deposit.deposit_id = deposit.id
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.region_department ON user.user_department_code = region_department.num_dep
    LEFT JOIN actual_amount_spent ON deposit.id = actual_amount_spent.deposit_id
    LEFT JOIN theoretical_amount_spent ON deposit.id = theoretical_amount_spent.deposit_id
    LEFT JOIN first_booking_date ON deposit.id = first_booking_date.deposit_id
    LEFT JOIN user_suspension_history ON user_suspension_history.userId = user.user_id
    and rank = 1
WHERE
    user_role IN ('UNDERAGE_BENEFICIARY', 'BENEFICIARY')
    AND (
        user.user_is_active
        OR user_suspension_history.reasonCode = 'UPON_USER_REQUEST'
    )