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
  , Q1_Topics as topics
  , anciennete_jours
  , non_cancelled_bookings
  , offers_created
FROM `{{ bigquery_raw_dataset }}.qualtrics_answers_ir_survey_pro`