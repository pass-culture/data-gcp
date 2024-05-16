WITH days AS (
    SELECT
        *
    FROM
        UNNEST(
            GENERATE_DATE_ARRAY('2019-02-11', CURRENT_DATE, INTERVAL 1 DAY)
        ) AS day
),
user_active_dates AS (
    SELECT
        user_id,
        user_department_code,
        user_region_name,
        user_birth_date,
        deposit_id,
        deposit_amount,
        deposit_creation_date,
        deposit_type,
        days.day AS active_date,
        DATE_DIFF(CURRENT_DATE, deposit_creation_date, DAY) AS seniority_days,
        DATE_DIFF(CURRENT_DATE, deposit_creation_date, MONTH) AS seniority_months
    FROM
        {{ ref('enriched_deposit_data') }} AS enriched_deposit_data
        JOIN days ON days.day BETWEEN DATE(enriched_deposit_data.deposit_creation_date)
        AND DATE(enriched_deposit_data.deposit_expiration_date)
),
aggregated_daily_user_used_bookings_history_1 AS (
    SELECT
        user_active_dates.active_date,
        user_active_dates.user_id,
        user_active_dates.user_department_code,
        user_active_dates.user_region_name,
        IF(EXTRACT(DAYOFYEAR FROM user_active_dates.active_date) < EXTRACT(DAYOFYEAR FROM user_birth_date),
            DATE_DIFF(user_active_dates.active_date, user_birth_date, YEAR) - 1,
            DATE_DIFF(user_active_dates.active_date, user_birth_date, YEAR))  AS user_age,
        user_active_dates.deposit_id,
        user_active_dates.deposit_type,
        user_active_dates.deposit_amount AS initial_deposit_amount,
        seniority_days,
        seniority_months,
        DATE_DIFF(
            user_active_dates.active_date,
            deposit_creation_date,
            DAY
        ) AS days_since_deposit_created,
        DATE_DIFF(
            user_active_dates.active_date,
            deposit_creation_date,
            MONTH
        ) AS months_since_deposit_created,
        COALESCE(SUM(booking_intermediary_amount), 0) AS amount_spent,
        COUNT(booking_id) AS cnt_used_bookings
    FROM
        user_active_dates
        LEFT JOIN {{ ref('mrt_global__booking') }} ebd ON ebd.deposit_id = user_active_dates.deposit_id
        AND user_active_dates.active_date = DATE(booking_used_date)
        AND booking_is_used
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12
),
-- Ajouter score de diversification par jour et score cumulé
-- /!\ Score de diversification indisponible pour les résas sur les offres inexistantes dans la table enriched_offer_data
diversification as (
  SELECT
    divers.user_id
    , divers.booking_id
    , divers.booking_creation_date
    , date(booking_used_date) as booking_used_date
    , delta_diversification
  FROM {{ ref('diversification_booking') }} divers
  LEFT JOIN {{ ref('mrt_global__booking') }} book
  ON divers.booking_id = book.booking_id
),

diversification_used as (
  -- Aggreger le score de diversification selon la date d'utilisation du booking
  SELECT
    user_id
    , booking_used_date
    , count(booking_id) as nb_bookings
    , sum(delta_diversification) AS delta_diversification
  FROM diversification
  GROUP BY
    user_id
    , booking_used_date
)
SELECT
    active_date,
    activity.user_id,
    user_department_code,
    user_region_name,
    user_age,
    deposit_id,
    deposit_type,
    initial_deposit_amount,
    seniority_days,
    seniority_months,
    days_since_deposit_created,
    months_since_deposit_created,
    amount_spent,
    cnt_used_bookings,
    SUM(amount_spent) OVER (
        PARTITION BY activity.user_id,
        deposit_id
        ORDER BY
            active_date ASC
    ) AS cumulative_amount_spent,
    SUM(cnt_used_bookings) OVER (
        PARTITION BY activity.user_id,
        deposit_id
        ORDER BY
            active_date
    ) AS cumulative_cnt_used_bookings
    , coalesce(delta_diversification, 0) as delta_diversification
    , coalesce(sum(delta_diversification) over(partition by activity.user_id order by active_date), 0) as delta_diversification_cumsum
FROM
    aggregated_daily_user_used_bookings_history_1 as activity
LEFT JOIN diversification_used
  ON activity.user_id = diversification_used.user_id
  AND activity.active_date = diversification_used.booking_used_date
