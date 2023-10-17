SELECT 
    n_ms4_id AS level_id,
    n_ms4_cod AS level_code,
    n_ms4_lib AS level_lib
FROM  `{{ bigquery_raw_dataset }}.institutional_scholar_level`