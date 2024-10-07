with base as (
select distinct
  start_date
  , end_date
  , response_id
  , user_id as venue_id
  , case
  when survey_id = 'SV_3IdnHqrnsuS17oy' then 'GRANT_15_17'
  when survey_id = 'SV_cBV3xaZ92BoW5sW' then 'GRANT_18'
  end as user_type
  , CASE
    WHEN question = "Q3 - Topics" THEN "Q3_topics"
  end as question
  , question_id
  , question_str
  , REPLACE(extra_data, "nan", "'nan'") AS extra_data
from `{{ bigquery_raw_dataset }}.qualtrics_answers`
where survey_id in ("SV_3IdnHqrnsuS17oy", "SV_cBV3xaZ92BoW5sW")
and question in ('Q1', 'Q2', 'Q3', 'Q3 - Topics'))

select *
, TRIM(JSON_EXTRACT(extra_data, '$.theoretical_amount_spent'), '"') as theoretical_amount_spent
, TRIM(JSON_EXTRACT(extra_data, '$.user_activity'), '"') as user_activity
, TRIM(JSON_EXTRACT(extra_data, '$.user_civility'), '"') as user_civility
from base
