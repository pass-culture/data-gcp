WITH
  weeks AS (
  SELECT
    *
  FROM
    UNNEST( GENERATE_DATE_ARRAY('2021-05-20', CURRENT_DATE, INTERVAL 1 WEEK) ) AS week ),
  deposit_active_weeks AS (
  SELECT
    `analytics_prod.enriched_deposit_data`.user_id,
    user_department_code,
    user_region_name,
    user_birth_date,
    deposit_id,
    deposit_amount,
    DATE_TRUNC(deposit_creation_date, WEEK(MONDAY)) AS deposit_creation_week,
    deposit_type,
    weeks.week AS active_week,
    DATE_DIFF(CURRENT_DATE, deposit_creation_date, WEEK(MONDAY)) AS seniority_weeks,
  FROM
    `{{ bigquery_analytics_dataset }}.enriched_deposit_data` enriched_deposit_data
  INNER JOIN
    weeks
  ON
    weeks.week BETWEEN DATE(enriched_deposit_data.deposit_creation_date)
    AND DATE(enriched_deposit_data.deposit_expiration_date) -- Toutes les semaines de vie du crédit
    AND deposit_creation_date > '2021-05-20' -- Les utilisateurs post sortie de l'app mobile
  INNER JOIN
    `{{ bigquery_analytics_dataset }}.firebase_aggregated_users` fau
  ON
    enriched_deposit_data.user_id = fau.user_id
    AND DATE(last_connexion_date) >= DATE(deposit_creation_date) ), -- Uniquement des utilisateurs connectés post octroi du crédit

  aggregated_weekly_deposit_bookings_history AS (
  SELECT
    deposit_active_weeks.active_week,
    deposit_active_weeks.deposit_creation_week,
    deposit_active_weeks.user_id,
    deposit_active_weeks.user_department_code,
    deposit_active_weeks.user_region_name,
  IF
    (EXTRACT(DAYOFYEAR
      FROM
        deposit_active_weeks.active_week) < EXTRACT(DAYOFYEAR
      FROM
        user_birth_date), DATE_DIFF(deposit_active_weeks.active_week, user_birth_date, YEAR) - 1, DATE_DIFF(deposit_active_weeks.active_week, user_birth_date, YEAR)) AS user_age,
    deposit_active_weeks.deposit_id,
    deposit_active_weeks.deposit_type,
    deposit_active_weeks.deposit_amount,
    seniority_weeks,
    DATE_DIFF( deposit_active_weeks.active_week, deposit_creation_week, WEEK(MONDAY) ) AS weeks_since_deposit_created,
    DATE_DIFF( deposit_active_weeks.active_week, deposit_creation_week, MONTH ) AS months_since_deposit_created,
    COALESCE(SUM(booking_intermediary_amount), 0) AS amount_spent,
    COALESCE(COUNT(ebd.booking_id),0) AS cnt_no_cancelled_bookings,
    COALESCE(SUM(delta_diversification),0) AS delta_diversification
  FROM
    deposit_active_weeks
  LEFT JOIN
    `{{ bigquery_analytics_dataset }}.enriched_booking_data` ebd
  ON
    ebd.deposit_id = deposit_active_weeks.deposit_id
    AND deposit_active_weeks.active_week = DATE_TRUNC(booking_creation_date, WEEK(MONDAY))
    AND NOT booking_is_cancelled
  LEFT JOIN
    `{{ bigquery_analytics_dataset }}.diversification_booking` diversification_booking
  ON
    diversification_booking.booking_id = ebd.booking_id
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
    12 ),

  cum_booking_history AS (
  SELECT
    active_week,
    activity.user_id,
    activity.deposit_id,
    user_department_code,
    user_region_name,
    user_age,
    deposit_creation_week,
    deposit_type,
    deposit_amount,
    seniority_weeks,
    weeks_since_deposit_created,
    months_since_deposit_created,
    amount_spent,
    cnt_no_cancelled_bookings,
    COALESCE(SUM(amount_spent) OVER (PARTITION BY activity.deposit_id ORDER BY active_week ASC ),0) AS cumulative_amount_spent,
    COALESCE(SUM(cnt_no_cancelled_bookings) OVER (PARTITION BY deposit_id ORDER BY active_week ),0) AS cumulative_cnt_no_cancelled_bookings,
    COALESCE(delta_diversification, 0) AS delta_diversification,
    COALESCE(SUM(delta_diversification) OVER(PARTITION BY activity.deposit_id ORDER BY active_week), 0) AS delta_diversification_cumsum
  FROM
    aggregated_weekly_deposit_bookings_history AS activity ),

  visits_and_conversion AS (
  SELECT
    active_week,
    cum_booking_history.user_id,
    user_department_code,
    user_region_name,
    user_age,
    cum_booking_history.deposit_id,
    deposit_creation_week,
    deposit_type,
    deposit_amount,
    seniority_weeks,
    weeks_since_deposit_created,
    months_since_deposit_created,
    amount_spent,
    cnt_no_cancelled_bookings,
    cumulative_amount_spent,
    cumulative_cnt_no_cancelled_bookings,
    delta_diversification,
    delta_diversification_cumsum,
    COALESCE(COUNT(DISTINCT session_id),0) AS nb_visits,
    COALESCE(COUNT(DISTINCT DATE_TRUNC(DATE(first_event_timestamp), DAY)),0) AS nb_distinct_days_visits,
    COALESCE(COUNT(DISTINCT
        CASE
          WHEN firebase_session_origin.traffic_campaign IS NOT NULL THEN session_id
        ELSE
        NULL
      END
        ),0) AS nb_visits_marketing,
    COALESCE(SUM(nb_consult_offer),0) AS nb_consult_offer,
    COALESCE(SUM(nb_booking_confirmation),0) AS nb_booking_confirmation,
    COALESCE(SUM(nb_add_to_favorites),0) AS nb_add_to_favorites,
    COALESCE(SUM(visit_duration_seconds),0) AS visit_duration_seconds
  FROM
    cum_booking_history
  LEFT JOIN
    `{{ bigquery_analytics_dataset }}.firebase_visits` firebase_visits
  ON
    firebase_visits.user_id = cum_booking_history.user_id
    AND DATE_TRUNC(DATE(firebase_visits.first_event_timestamp), WEEK(MONDAY)) = cum_booking_history.active_week
  LEFT JOIN
    `{{ bigquery_analytics_dataset }}.firebase_session_origin` firebase_session_origin
  USING
    (user_pseudo_id,
      session_id)
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
    12,
    13,
    14,
    15,
    16,
    17,
    18 ),

  visits_ranked AS (
  SELECT
    deposit_id,
    active_week,
    ROW_NUMBER() OVER(PARTITION BY deposit_id ORDER BY active_week) AS visit_rank
  FROM
    visits_and_conversion
  WHERE
    nb_visits > 0 )

SELECT
  *,
  LAG(nb_visits) OVER(PARTITION BY user_id ORDER BY active_week) AS visits_previous_week,
  LAG(nb_consult_offer) OVER(PARTITION BY user_id ORDER BY active_week) AS consult_previous_week
FROM
  visits_and_conversion
LEFT JOIN
  visits_ranked
USING
  (deposit_id,
    active_week)