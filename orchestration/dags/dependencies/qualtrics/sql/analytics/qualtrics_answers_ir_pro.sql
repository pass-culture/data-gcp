with
    base as (
        select
            start_date,
            end_date,
            response_id,
            user_id as venue_id,
            'pro' as user_type,
            case
                when question = "Q1 - Topics"
                then "Q1_topics"
                when question = "Q1 - Parent Topics"
                then "Q1_parent_topics"
                when question = "Q1"
                then "Q1"
                when question = "Q3"
                then "Q3"
                when question = "Q1_NPS_GROUP"
                then "Q1_nps_group"
            end as question,
            question_id,
            question_str,
            answer,
            replace(replace(extra_data, "d'un", "dun"), "nan", "'nan'") as extra_data
        from `{{ bigquery_raw_dataset }}.qualtrics_answers`
        where survey_id = "SV_eOOPuFjgZo1emR8"
    )

select
    *,
    trim(json_extract(extra_data, '$.anciennete_jours'), '"') as anciennete_jours,
    trim(
        json_extract(extra_data, '$.non_cancelled_bookings'), '"'
    ) as non_cancelled_bookings,
    trim(json_extract(extra_data, '$.offers_created'), '"') as offers_created
from base
