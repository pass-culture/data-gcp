with
    raw_answers as (
        select
            raw_answers.user_id,
            submitted_at,
            answers,
            cast(null as string) as catch_up_user_id
        from {{ source("raw", "qpi_answers_v4") }} raw_answers
    ),

    base as (select * from (select * from raw_answers) as qpi, qpi.answers as answers),

    unnested_base as (
        select user_id, submitted_at, unnested as answer_ids
        from base
        cross join unnest(base.answer_ids) as unnested
    ),

    user_subcat as (
        select b.user_id, b.submitted_at, map.subcategories
        from unnested_base b
        join {{ source("seed", "qpi_mapping") }} map on b.answer_ids = map.answer_id
        where b.answer_ids not like 'PROJECTION_%'
    ),
    clean as (
        select user_id, submitted_at, unnested.element as subcategories
        from user_subcat
        cross join unnest(user_subcat.subcategories.list) as unnested
    ),
    base_deduplicate as (
        select
            user_id,
            submitted_at,
            subcategories,
            row_number() over (
                partition by user_id, subcategories order by subcategories desc
            ) row_number
        from clean
    )
select * except (row_number)
from base_deduplicate
where row_number = 1 and subcategories is not null and subcategories <> ""
