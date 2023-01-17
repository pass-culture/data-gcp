SELECT *
FROM `{{ bigquery_clean_dataset }}.dms_{{ params.target }}`
QUALIFY ROW_NUMBER() OVER (PARTITION BY application_number ORDER BY last_update_at DESC) = 1
