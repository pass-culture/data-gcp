select
    startdate as start_date,
    enddate as end_date,
    responseid as response_id,
    externalreference as venue_id,
    user_type,
    question,
    question_id,
    question_str,
    answer,
    execution_date,
    q1_topics as topics,
    anciennete_jours,
    non_cancelled_bookings,
    offers_created
from `{{ bigquery_raw_dataset }}.qualtrics_answers_ir_survey_pro`
