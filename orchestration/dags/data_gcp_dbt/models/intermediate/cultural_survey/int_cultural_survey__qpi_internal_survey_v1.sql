with
    exploded_answers as (
        select
            a.culturalsurvey_id,
            a.form_id,
            a.submitted_at,
            a.landed_at,
            a.platform,
            ans.question_id,
            ans.choice,
            ans.choices as choice_array
        from {{ source("gcs_seeds", "qpi_answers_v1") }} as a, unnest(a.answers) as ans
    ),

    aggregated_answers as (
        select
            culturalsurvey_id,
            form_id,
            submitted_at,
            landed_at,
            platform,
            question_id,
            array_concat(
                array(select c from unnest([choice]) as c where c is not null),  -- single choice if exists
                choice_array  -- multiple choices if exists
            ) as all_answers
        from exploded_answers
    )

select
    u.user_id,
    aa.culturalsurvey_id,
    aa.form_id,
    aa.submitted_at,
    aa.landed_at,
    aa.platform,
    aa.question_id,
    q.question_text,
    aa.all_answers
from aggregated_answers as aa
left join {{ ref("int_seed__qpi_questions_v1") }} as q on aa.question_id = q.question_id
left join
    {{ ref("int_applicative__user") }} as u
    on aa.culturalsurvey_id = u.user_cultural_survey_id
where u.user_has_filled_cultural_survey
order by u.user_id, aa.question_id
