WITH last_day_of_month AS (
  SELECT
    date_trunc(day, MONTH) as month,
    max(day) as last_active_date
  FROM  `{{ bigquery_analytics_dataset }}.retention_partner_history`
  GROUP BY 1
),

all_partners AS (
  SELECT
    DATE_TRUNC(day, MONTH) AS month,
    "{{ params.group_type }}" AS dimension_name,
    {% if params.group_type == 'NAT' %} 'NAT' {% else %} {{ params.group_type_name }} {% endif %} AS dimension_value,
    COUNT(distinct if(  DATE_DIFF(day, last_bookable_date,DAY) <= 365, retention_partner_history.partner_id, null)) AS numerator,
    COUNT(distinct if(retention_partner_history.first_offer_creation_date <= day, retention_partner_history.partner_id, null)) AS denominator,
  FROM `{{ bigquery_analytics_dataset }}.retention_partner_history` retention_partner_history 
  INNER JOIN last_day_of_month ldm on ldm.last_active_date = retention_partner_history.day 
  LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_cultural_partner_data` enriched_cultural_partner_data ON retention_partner_history.partner_id  = enriched_cultural_partner_data.partner_id 
  LEFT JOIN `{{ bigquery_analytics_dataset }}.region_department` region_department ON enriched_cultural_partner_data.partner_department_code = region_department.num_dep
  GROUP BY
    1,
    2,
    3
)
SELECT
  month,
  dimension_name,
  dimension_value,
  NULL AS user_type,
  "taux_retention_partenaires" AS indicator,
  denominator,
  numerator
FROM all_partners
WHERE month >= "2023-01-01"
