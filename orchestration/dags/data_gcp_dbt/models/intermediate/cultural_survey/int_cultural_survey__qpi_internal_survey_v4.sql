with
    mapped_to_subcategories as (
        select
            ra.user_id, ra.submitted_at, ra.question_id, ra.answer_id, map.subcategories
        from {{ ref("int_pcapi__raw_qpi_answer_v4") }} as ra
        inner join
            {{ source("seed", "qpi_mapping") }} as map on ra.answer_id = map.answer_id
    ),

    unnested_subcategories as (
        select
            map.user_id,
            map.submitted_at,
            map.question_id,
            map.answer_id,
            unnested.element as subcategory_id
        from mapped_to_subcategories as map
        cross join unnest(map.subcategories.list) as unnested
    ),

    with_categories as (
        select
            us.user_id,
            us.submitted_at,
            us.question_id,
            us.answer_id,
            subcat.category_id,
            us.subcategory_id
        from unnested_subcategories as us
        inner join
            {{ source("raw", "subcategories") }} as subcat
            on us.subcategory_id = subcat.id
    )

select
    awc.user_id,
    awc.submitted_at,
    awc.question_id,
    awc.answer_id,
    '4' as qpi_version,
    array_agg(distinct awc.category_id) as category_ids,
    array_agg(distinct awc.subcategory_id) as subcategory_ids
from with_categories as awc
group by awc.user_id, awc.submitted_at, awc.question_id, awc.answer_id
