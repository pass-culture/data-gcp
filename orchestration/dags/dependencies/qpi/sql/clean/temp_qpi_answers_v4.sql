SELECT 
    raw_answers.user_id
    , submitted_at
    , answers
    , CAST(NULL AS STRING) AS catch_up_user_id
FROM `{{ bigquery_raw_dataset }}.temp_qpi_answers_v4` raw_answers