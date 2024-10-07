select
    startdate as start_date,
    enddate as end_date,
    responseid as response_id,
    externalreference as user_id,
    user_type,
    question,
    question_id,
    question_str,
    answer,
    execution_date,
    q3_topics as topics,
    theoretical_amount_spent,
    user_activity,
    user_civility
from `{{ bigquery_raw_dataset }}.qualtrics_answers_ir_survey_jeunes`
qualify row_number() over (partition by response_id order by execution_date desc) = 1
