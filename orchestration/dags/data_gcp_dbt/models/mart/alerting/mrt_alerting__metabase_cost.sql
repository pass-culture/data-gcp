{{
    config(
        materialized="table",
        tags=["weekly"],
        labels={"schedule": "weekly"},
    )
}}

WITH temp_card AS (
  SELECT DISTINCT
    card_id,
    dashboard_name,
    ROW_NUMBER() OVER (PARTITION BY card_id ORDER BY date DESC) AS rank,
    MAX(date) OVER (PARTITION BY card_id) AS consultation_date
  FROM {{ ref("mrt_monitoring__metabase_cost") }}
  WHERE
    dashboard_id IS NOT null
)

SELECT DISTINCT
  cost.card_name,
  cost.card_id,
  temp_card.dashboard_name,
  SUM(cost.total_queries) OVER (PARTITION BY cost.card_id) AS total_views,
  COUNT(DISTINCT cost.metabase_user_id) OVER (PARTITION BY cost.card_id) AS total_distinct_users,
  SUM(cost.cost_euro) OVER (PARTITION BY cost.card_id) AS total_cost,
  AVG(cost.cost_euro) OVER (PARTITION BY cost.card_id) AS avg_cost
FROM {{ ref("mrt_monitoring__metabase_cost") }} AS cost
LEFT JOIN temp_card
ON cost.card_id = temp_card.card_id
AND temp_card.rank=1
WHERE cost.card_id IS NOT null
AND LOWER(cost.card_name) NOT LIKE '%archive%'
AND temp_card.consultation_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
