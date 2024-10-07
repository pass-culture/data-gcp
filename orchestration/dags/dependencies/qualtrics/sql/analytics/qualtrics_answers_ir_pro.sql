with base as (
select
  start_date
  , end_date
  , response_id
  , user_id as venue_id
  , 'pro' as user_type
  , CASE
    WHEN question = "Q1 - Topics" THEN "Q1_topics"
    WHEN question = "Q1 - Parent Topics" THEN "Q1_parent_topics"
    WHEN question = "Q1" THEN "Q1"
    WHEN question = "Q3" THEN "Q3"
    WHEN question = "Q1_NPS_GROUP" THEN "Q1_nps_group"
  end as question
  , question_id
  , question_str
  , answer
  , replace(REPLACE(extra_data, "d'un", "dun"), "nan", "'nan'") AS extra_data
from `{{ bigquery_raw_dataset }}.qualtrics_answers`
where survey_id = "SV_eOOPuFjgZo1emR8")

select *
, TRIM(JSON_EXTRACT(extra_data, '$.anciennete_jours'), '"') as anciennete_jours
, TRIM(JSON_EXTRACT(extra_data, '$.non_cancelled_bookings'), '"') as non_cancelled_bookings
, TRIM(JSON_EXTRACT(extra_data, '$.offers_created'), '"') as offers_created
from base
