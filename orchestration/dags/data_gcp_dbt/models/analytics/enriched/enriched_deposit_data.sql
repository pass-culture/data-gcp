WITH ranked_deposit AS (
    SELECT
        userId,
        id,
        ROW_NUMBER() OVER(
            PARTITION BY userId
            ORDER BY
                dateCreated,
                id
        ) AS deposit_rank_asc,

    ROW_NUMBER() OVER(
            PARTITION BY userId
            ORDER BY
               dateCreated DESC,
                id DESC
        ) AS deposit_rank_desc
    FROM
        {{ source('raw', 'applicative_database_deposit') }}
),

actual_amount_spent AS (
    SELECT
        deposit_id,
            SUM(
                booking_amount * booking_quantity
            ) AS deposit_actual_amount_spent
    FROM
        {{ ref('booking') }}
    WHERE booking_is_used IS TRUE
    AND booking_is_cancelled IS FALSE
    GROUP BY
        deposit_id
),

theoretical_amount_spent AS (
    SELECT
        deposit_id,
        SUM(booking_amount * booking_quantity) AS deposit_theoretical_amount_spent
    FROM
        {{ ref('booking') }}
    WHERE booking_is_cancelled IS FALSE
    GROUP BY deposit_id
),
theoretical_amount_spent_in_digital_goods AS (
    SELECT
        booking.deposit_id,
        SUM(booking.booking_amount * booking.booking_quantity) AS deposit_theoretical_amount_spent_in_digital_goods
    FROM
        {{ ref('booking') }} AS booking
        LEFT JOIN {{ ref('stock') }} AS stock ON booking.stock_id = stock.stock_id
        LEFT JOIN {{ ref('offer') }} AS offer ON stock.offer_id = offer.offer_id
        INNER JOIN {{ source('clean','subcategories') }} AS subcategories ON offer.offer_subcategoryId = subcategories.id
    WHERE
        subcategories.is_digital_deposit = true
        AND offer.offer_url IS NOT NULL
        AND booking.booking_is_cancelled IS FALSE
    GROUP BY
        booking.deposit_id
),

first_booking_date AS (
    SELECT
        deposit_id,
        MIN(booking_creation_date) AS deposit_first_booking_date,
        MAX(booking_creation_date) AS deposit_last_booking_date,
        COUNT(DISTINCT booking_id) AS deposit_no_cancelled_bookings
    FROM
        {{ ref('booking') }} AS booking
    WHERE booking_is_cancelled IS FALSE
    GROUP BY
        deposit_id
)

SELECT
    deposit.id AS deposit_id,
    deposit.amount AS deposit_amount,
    deposit.userId AS user_id,
    user.user_civility,
    user.user_department_code,
    user.user_age,
    region_department.region_name AS user_region_name,
    CASE WHEN lower(deposit.source) like "%educonnect%" THEN "EDUCONNECT" -- faire une macro ici
    WHEN lower(deposit.source) like "%ubble%" THEN "UBBLE"
    WHEN (lower(deposit.source) like "%dms%" OR lower(deposit.source) like "%démarches simplifiées%") THEN "DMS"
    ELSE deposit.source END
    AS deposit_source,
    user.user_creation_date AS user_creation_date,
    deposit.dateCreated AS deposit_creation_date,
    deposit.dateUpdated AS deposit_update_date,
    deposit.expirationDate AS deposit_expiration_date,
    deposit.type AS deposit_type,
    ranked_deposit.deposit_rank_asc,
    ranked_deposit.deposit_rank_desc,
    COALESCE(deposit_theoretical_amount_spent,0),
    COALESCE(deposit_actual_amount_spent,0),
    COALESCE(deposit_theoretical_amount_spent_in_digital_goods,0),
    fb.deposit_no_cancelled_bookings,
    fb.deposit_first_booking_date,
    fb.deposit_last_booking_date,
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
    {{ source('raw', 'applicative_database_deposit') }} AS deposit
    INNER JOIN {{ ref('user_beneficiary') }} AS user ON user.user_id = deposit.userId
    INNER JOIN ranked_deposit ON ranked_deposit.id = deposit.id
    LEFT JOIN {{ source('analytics','region_department') }} AS region_department ON user.user_department_code = region_department.num_dep
    LEFT JOIN actual_amount_spent AS a ON deposit.id = a.deposit_id
    LEFT JOIN theoretical_amount_spent AS tas ON deposit.id = tas.deposit_id
    LEFT JOIN theoretical_amount_spent_in_digital_goods AS tasdg ON deposit.id = tasdg.deposit_id
    LEFT JOIN first_booking_date AS fb ON deposit.id = first_booking_date.deposit_id
    LEFT JOIN {{ ref('user_suspension') }} AS user_suspension ON user_suspension.user_id = user.user_id
WHERE
    (
        user.user_is_active
        OR user_suspension.action_history_reason = 'upon user request'
    )