SELECT *
FROM `{{ biquery_clean_dataset }}.dms_{{ target }}`
QUALIFY ROW_NUMBER() OVER (PARTITION BY application_number ORDER BY last_update_at DESC) = 1
