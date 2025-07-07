{% set survey_id_pro = "SV_eOOPuFjgZo1emR8" %}
with
    base as (
        select
            start_date,
            end_date,
            response_id,
            user_id as venue_id,
            'pro' as user_type,
            question_id,
            question_str,
            answer,
            case
                when question = 'Q1 - Topics'
                then 'Q1_topics'
                when question = 'Q1 - Parent Topics'
                then 'Q1_parent_topics'
                when question = 'Q1'
                then 'Q1'
                when question = 'Q3'
                then 'Q3'
                when question = 'Q1_NPS_GROUP'
                then 'Q1_nps_group'
            end as question,
            replace(replace(extra_data, "d'un", 'dun'), 'nan', "'nan'") as extra_data,
            coalesce(
                question = 'Q1'
                and question_str
                = 'Recommanderiez-vous le pass Culture à une autre structure culturelle ?',
                false
            ) as is_nps_question
        from {{ source("raw", "qualtrics_answers") }}
        where survey_id = '{{ survey_id_pro }}'
    )

select
    *,
    trim(json_extract(extra_data, '$.anciennete_jours'), '"') as seniority_day_cnt,
    trim(
        json_extract(extra_data, '$.non_cancelled_bookings'), '"'
    ) as total_non_cancelled_bookings,
    trim(json_extract(extra_data, '$.offers_created'), '"') as total_offers_created
from base
qualify row_number() over (partition by response_id order by start_date desc) = 1
