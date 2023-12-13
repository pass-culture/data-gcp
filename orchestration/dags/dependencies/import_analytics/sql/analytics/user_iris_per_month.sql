WITH current_month AS (

SELECT 
  DATE_TRUNC(DATE('{{ ds }}'), month) AS month_log,
  user_declared_iris.user_id,
  user_declared_iris.iriscode as iris_declaree,
  user_declared_iris.department as department_declare,
  user_ip_iris.ip_iris,
  user_ip_iris.nb_log_ip,
  user_ip_iris.department AS ip_department,
  user_reco_iris.reco_iris,
  user_reco_iris.nb_log_reco,
  user_reco_iris.department AS reco_department
FROM `{{ bigquery_clean_dataset }}.user_declared_iris` AS user_declared_iris 
LEFT JOIN `{{ bigquery_clean_dataset }}.user_reco_iris` AS user_reco_iris ON user_reco_iris.user_id = user_declared_iris.user_id AND user_reco_iris.month_log = date_trunc(DATE('{{ ds }}'), month)
LEFT JOIN `{{ bigquery_clean_dataset }}.user_ip_iris` AS user_ip_iris ON user_ip_iris.user_id = user_declared_iris.user_id AND user_ip_iris.month_log = date_trunc(DATE('{{ ds }}'), month)
)

, last_month AS (
SELECT 
  date_trunc(DATE('{{ ds }}'), month) AS month_log,
  user_id,
  actual_iris as actual_iris_last_month,
  most_freq_iris as most_freq_iris_last_month,
  actual_department as actual_department_last_month,
  most_freq_department as most_freq_department_last_month,
  iris_score,
  department_score
FROM `{{ bigquery_analytics_dataset }}.user_iris_per_month` 
WHERE month_log = DATE_SUB(date_trunc(DATE('{{ ds }}'), month), interval 1 month) 
)

, almost AS (
SELECT 
  current_month.month_log,
  current_month.user_id,
  current_month.iris_declaree,
  CASE WHEN nb_log_ip > nb_log_reco THEN ip_iris ELSE reco_iris END AS most_freq_iris,
  CASE WHEN nb_log_ip > nb_log_reco THEN ip_department ELSE reco_department END AS most_freq_department,
  actual_iris_last_month,
  most_freq_iris_last_month,
  actual_department_last_month,
  most_freq_department_last_month,
  iris_score as last_month_iris_score,
  department_score as last_month_department_score
FROM current_month  
LEFT JOIN last_month ON current_month.user_id = last_month.user_id
)

SELECT 
  month_log,
  user_id,
  iris_declaree,
  CASE WHEN most_freq_iris IS NULL THEN most_freq_iris_last_month ELSE most_freq_iris END AS most_freq_iris,
  most_freq_department,
  CASE 
  WHEN most_freq_iris = most_freq_iris_last_month AND most_freq_iris IS NOT NULL THEN most_freq_iris 
  ELSE actual_iris_last_month
  END 
  AS actual_iris,
  CASE 
  WHEN most_freq_department = most_freq_department_last_month AND most_freq_department IS NOT NULL THEN most_freq_department 
  ELSE actual_department_last_month
  END 
  AS actual_department,
  CASE WHEN most_freq_iris = most_freq_iris_last_month THEN 1.0 
  WHEN (most_freq_iris = actual_iris_last_month OR most_freq_iris_last_month = actual_iris_last_month) THEN 1.0 
  ELSE last_month_iris_score / 2 END 
  AS iris_score,
  CASE WHEN most_freq_department = most_freq_department_last_month THEN 1.0 
  WHEN (most_freq_department = actual_department_last_month OR most_freq_department_last_month = actual_department_last_month) THEN 1.0 
  ELSE last_month_department_score / 2 END 
  AS department_score,
  ST_DISTANCE(param_iris_most_freq.centroid, param_iris_declaree.centroid) distance_iris_actuelle_et_iris_declaree

FROM almost 
LEFT JOIN `{{ bigquery_clean_dataset }}.iris_france` param_iris_most_freq on param_iris_most_freq.id = almost.most_freq_iris
LEFT JOIN `{{ bigquery_clean_dataset }}.iris_france` param_iris_declaree on param_iris_declaree.id = almost.iris_declaree


