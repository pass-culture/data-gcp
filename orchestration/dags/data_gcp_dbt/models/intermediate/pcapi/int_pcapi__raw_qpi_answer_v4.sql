with
    raw_answers as (
        select raw_answers.user_id, raw_answers.submitted_at, raw_answers.answers
        from {{ source("raw", "qpi_answers_v4") }} as raw_answers
    ),

    unnested_questions as (
        select user_id, submitted_at, answers.question_id, answers.answer_ids
        from raw_answers
        cross join unnest(answers) as answers
    ),

    unnested_answers as (
        select user_id, submitted_at, question_id, answer_id
        from unnested_questions
        cross join unnest(answer_ids) as answer_id
    )

select user_id, submitted_at, question_id, answer_id
from unnested_answers
