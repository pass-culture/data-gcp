with population_dpt as (
  SELECT  
      DATE(pop.current_date) active_month,
      pop.decimal_age,
      DATE(pop.born_date) as born_date,
      pop.department_code,
      pop.department_name,
      dep.region_name,
      sum(population) as population
    FROM `{{ bigquery_analytics_dataset }}.population_age_and_department_france_details` pop
    LEFT JOIN `{{ bigquery_analytics_dataset }}.region_department` dep	on dep.num_dep = pop.department_code
    WHERE pop.current_year in (2020, 2021, 2022) and cast(age as int) BETWEEN 15 AND 25
    GROUP BY 1,2,3,4,5,6
  ),

user_booking AS ( 
  SELECT
    aa.active_month,
    aa.user_department_code as department_code,
    DATE(DATE_TRUNC(ud.user_birth_date , MONTH)) as born_date,      
    COUNT(distinct ud.user_id) as total_users,
    FROM  `{{ bigquery_analytics_dataset }}.aggregated_monthly_user_used_booking_activity` aa
    INNER JOIN `{{ bigquery_analytics_dataset }}.enriched_user_data` ud on ud.user_id = aa.user_id
  GROUP BY 1,2,3
)


SELECT 
  pop.active_month,
  pop.born_date,
  pop.decimal_age,
  pop.department_code,
  ub.total_users,
  CASE
   WHEN decimal_age >= 15 AND decimal_age < 18 THEN "15_17" 
   WHEN decimal_age >= 18 AND decimal_age < 20 THEN "18_19" 
  ELSE "20_25"
  END AS age_range,
  population
FROM population_dpt pop
LEFT JOIN user_booking ub on 
    pop.active_month = ub.active_month
    AND pop.born_date = ub.born_date
    AND pop.department_code = ub.department_code