WITH last_day_of_month AS (
  SELECT
    date_trunc(active_date, MONTH) as month,
    max(active_date) as last_active_date
  FROM  `{{ bigquery_analytics_dataset }}.aggregated_daily_user_used_activity`
  GROUP BY 1
),

aggregated_active_beneficiary AS (
    SELECT
        month
        , "{{ params.group_type }}" as dimension_name
        , {% if params.group_type == 'NAT' %}
            'NAT'
        {% else %}
            {{ params.group_type_name }}
        {% endif %} as dimension_value
        , deposit_type as user_type
        , "beneficiaire_actuel" as indicator
        , COUNT(DISTINCT uua.user_id) as numerator 
        , 1 as denominator      
  FROM
      `{{ bigquery_analytics_dataset }}.aggregated_daily_user_used_activity` uua
  INNER JOIN last_day_of_month ldm on ldm.last_active_date = active_date 
  -- active nor suspended
  INNER JOIN `{{ bigquery_analytics_dataset }}.enriched_user_data` eud ON eud.user_id = uua.user_id 
  LEFT JOIN `{{ bigquery_analytics_dataset }}.region_department` as rd
        on  eud.user_department_code = rd.num_dep 
  -- still have some credit at EOM
  WHERE cumulative_amount_spent < initial_deposit_amount

  GROUP BY  1, 2, 3, 4, 5
),

aggregated_total_beneficiairy AS (
    SELECT
        month
        , "{{ params.group_type }}" as dimension_name
        , {% if params.group_type == 'NAT' %}
            'NAT'
        {% else %}
            {{ params.group_type_name }}
        {% endif %} as dimension_value
        , cast(null as string) as user_type
        , "beneficiaire_total" as indicator
        , COUNT(DISTINCT  eud.user_id) as numerator  
        , 1 as denominator
  FROM last_day_of_month ldm
  INNER JOIN `{{ bigquery_analytics_dataset }}.enriched_user_data` eud ON date(eud.user_deposit_creation_date) <= date(ldm.last_active_date) 
  LEFT JOIN `{{ bigquery_analytics_dataset }}.region_department` as rd
        on  eud.user_department_code = rd.num_dep 
  GROUP BY  1, 2, 3, 4, 5
)


SELECT *
FROM aggregated_active_beneficiary 
UNION ALL
SELECT *
FROM aggregated_total_beneficiairy