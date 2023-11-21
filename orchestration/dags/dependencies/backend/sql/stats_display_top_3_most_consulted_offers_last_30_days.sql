WITH
  consult_per_offer_last_3O_days AS (
  SELECT
    offerer_id,
    offer_id,
    SUM(cnt_events) AS nb_consult_last_30_days
  FROM
    `{{ bigquery_analytics_dataset }}.aggregated_daily_offer_consultation_data`
  WHERE
    event_date between DATE_SUB('{{ ds }}', INTERVAL 30 DAY) and DATE('{{ ds }}')
  AND
    event_name = 'ConsultOffer'
  GROUP BY
    1,
    2 )

SELECT 
  CAST("{{ ds }}" AS DATETIME) AS execution_date,
  offerer_id,
  offer_id,
  nb_consult_last_30_days,
  ROW_NUMBER() OVER(PARTITION BY offerer_id ORDER BY nb_consult_last_30_days DESC) AS consult_rank
FROM
  consult_per_offer_last_3O_days QUALIFY ROW_NUMBER() OVER(PARTITION BY offerer_id ORDER BY nb_consult_last_30_days DESC) <= 3
