WITH
  all_days_since_activation AS (
  SELECT
    DISTINCT offerer_id,
    offerer_creation_date,
    DATE_ADD(DATE(offerer_creation_date), INTERVAL
    OFFSET
      DAY) AS day
  FROM
    {{ ref('enriched_offerer_data') }}
  INNER JOIN
    {{ ref('aggregated_daily_offer_consultation_data') }}
  USING
    (offerer_id)
  CROSS JOIN
    UNNEST(GENERATE_ARRAY(0, DATE_DIFF(CURRENT_DATE(), '2018-01-01', DAY))) AS
  OFFSET
  WHERE
    DATE_ADD(DATE(offerer_creation_date), INTERVAL
    OFFSET
      DAY) >= offerer_creation_date -- Les jours depuis la création de la structure
    AND DATE_ADD(DATE(offerer_creation_date), INTERVAL
    OFFSET
      DAY) <= CURRENT_DATE() -- Que des jours avant aujourd'hui
      ),

    consult_per_offerer_and_day AS (
    SELECT
      event_date,
      offerer_id,
      SUM(cnt_events) AS nb_daily_consult
    FROM
     {{ ref('aggregated_daily_offer_consultation_data') }}
    WHERE
      event_name = 'ConsultOffer'
    GROUP BY
      1,
      2),
    cum_consult_per_day AS (
    SELECT
      *,
      SUM(nb_daily_consult) OVER(PARTITION BY offerer_id ORDER BY event_date) AS cum_consult
    FROM
      consult_per_offerer_and_day )
  SELECT
    day AS event_date,
    DATE_DIFF(current_date, day, DAY) AS day_seniority,
    all_days_since_activation.offerer_id,
    COALESCE(nb_daily_consult,0) AS nb_daily_consult,
    MAX(cum_consult) OVER(PARTITION BY all_days_since_activation.offerer_id ORDER BY day ROWS UNBOUNDED PRECEDING ) AS cum_consult
  FROM
    all_days_since_activation
  LEFT JOIN
    cum_consult_per_day
  ON
    all_days_since_activation.offerer_id = cum_consult_per_day.offerer_id
    AND all_days_since_activation.day = cum_consult_per_day.event_date
  WHERE
    DATE_DIFF(current_date, day, DAY) <= 180