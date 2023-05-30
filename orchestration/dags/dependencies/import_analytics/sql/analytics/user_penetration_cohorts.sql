
with cohorted_population as (
  SELECT 
    DATE(active_month) as active_month,
    CAST(decimal_age as STRING) as decimal_age,
    department_code,
    sum(total_users) total_users,
    sum(population) population
  FROM `{{ bigquery_analytics_dataset }}.user_penetration` 
  WHERE decimal_age in (15, 15.5, 16, 16.5, 17, 17.5, 18, 18.5, 19)
  group by 1,2,3
)

SELECT
  active_month,
  decimal_age,
  department_code,
  total_users,
  SUM(total_users) OVER (
    PARTITION BY decimal_age, department_code 
    ORDER BY active_month ROWS BETWEEN 11 PRECEDING AND current row 
  ) as total_users_last_12_months,
  population,
  SUM(population) OVER (
    PARTITION BY decimal_age, department_code 
    ORDER BY active_month ROWS BETWEEN 11 PRECEDING AND current row 
  ) as population_last_12_months,
  
FROM cohorted_population
