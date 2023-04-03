SELECT 
  StartDate as start_date
  , EndDate as end_date
  , ResponseId as response_id
  , ExternalReference as user_id
  , user_type
  , question
  , question_id
  , question_str
  , answer
  , execution_date
  , Q3_Topics as topics
  , theoretical_amount_spent
  , user_activity
  , user_civility
FROM `{{ bigquery_raw_dataset }}.qualtrics_answers_ir_survey_jeunes`
