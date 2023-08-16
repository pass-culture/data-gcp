WITH consult_per_offerer_and_day AS (
SELECT
    event_date
    , DATE_DIFF(current_date, event_date, DAY) AS day_seniority
    , offerer_id
    , SUM(cnt_consult_offer) AS nb_daily_consult
FROM `{{ bigquery_analytics_dataset }}.aggregated_daily_offer_consultation_data`
GROUP BY 1,2,3
),

cum_consult_per_day AS (
SELECT
    *
    , SUM(nb_daily_consult) OVER(PARTITION BY offerer_id ORDER BY event_date) AS cum_consult
FROM consult_per_offerer_and_day
)

SELECT
    event_date
    ,day_seniority
    ,offerer_id
    , nb_daily_consult
    ,cum_consult
FROM cum_consult_per_day
WHERE day_seniority <= 180