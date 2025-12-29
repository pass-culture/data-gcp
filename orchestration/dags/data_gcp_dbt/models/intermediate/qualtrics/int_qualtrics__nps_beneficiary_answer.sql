{% set survey_id_15_17 = "SV_3IdnHqrnsuS17oy" %}
{% set survey_id_18 = "SV_cBV3xaZ92BoW5sW" %}


with
    base as (
        select
            response_id,
            user_id,
            question_id,
            question_str,
            answer,
            date(start_date) as start_date,
            date(end_date) as end_date,
            case
                when survey_id = '{{ survey_id_15_17 }}'
                then 'GRANT_15_17'
                when survey_id = '{{ survey_id_18 }}'
                then 'GRANT_18'
            end as user_type,
            case when question = 'Q3 - Topics' then 'Q3_topics' end as question,
            replace(extra_data, 'nan', "'nan'") as extra_data,
            coalesce(
                question = 'Q1'
                and question_str
                = 'Recommanderais-tu le pass Culture à un ami ou un collègue ?',
                false
            ) as is_nps_question
        from {{ source("raw", "qualtrics_answers") }}
        where
            survey_id in ('{{ survey_id_15_17 }}', '{{ survey_id_18 }}')
            and question in ('Q1', 'Q2', 'Q3', 'Q3 - Topics')
    )

select
    response_id,
    user_id,
    question_id,
    question_str,
    answer,
    start_date,
    end_date,
    user_type,
    question,
    extra_data,
    is_nps_question,
    trim(
        json_extract(extra_data, '$.theoretical_amount_spent'), '"'
    ) as theoretical_amount_spent,
    trim(json_extract(extra_data, '$.user_activity'), '"') as user_activity,
    trim(json_extract(extra_data, '$.user_civility'), '"') as user_civility
from base
qualify row_number() over (partition by response_id order by start_date desc) = 1
