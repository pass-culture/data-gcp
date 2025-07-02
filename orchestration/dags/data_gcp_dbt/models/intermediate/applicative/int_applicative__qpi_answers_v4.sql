with
    raw_answers as (
        select
            raw_answers.user_id,
            raw_answers.submitted_at,
            raw_answers.answers,
            cast(null as string) as catch_up_user_id
        from {{ source("raw", "qpi_answers_v4") }} as raw_answers
    ),

    base as (select * from (select * from raw_answers) as qpi, qpi.answers as answers),

    unnested_base as (
        select b.user_id, b.submitted_at, unnested as answer_ids
        from base as b
        cross join unnest(b.answer_ids) as unnested
    ),

    user_subcat as (
        select b.user_id, b.submitted_at, map.subcategories
        from unnested_base as b
        inner join
            {{ source("seed", "qpi_mapping") }} as map on b.answer_ids = map.answer_id
        where b.answer_ids not like 'PROJECTION_%'
    ),

    clean as (
        select us.user_id, us.submitted_at, unnested.element as subcategories
        from user_subcat as us
        cross join unnest(user_subcat.subcategories.list) as unnested
    ),

    base_deduplicate as (
        select
            user_id,
            submitted_at,
            subcategories,
            row_number() over (
                partition by user_id, subcategories order by subcategories desc
            ) as row_number
        from clean
    )

select user_id, submitted_at, subcategories
from base_deduplicate
where row_number = 1 and subcategories is not null and subcategories <> ''
