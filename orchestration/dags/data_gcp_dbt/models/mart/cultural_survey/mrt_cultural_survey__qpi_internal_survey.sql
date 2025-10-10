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
            user_id,
            submitted_at,
            question_id,
            answer_id,
            unnested.element as subcategory_id
        from mapped_to_subcategories
        cross join unnest(subcategories.list) as unnested
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
    ),

    v4_with_category_arrays as (
        select
            user_id,
            submitted_at,
            question_id,
            answer_id,
            array_agg(distinct category_id) as category_ids,
            array_agg(distinct subcategory_id) as subcategory_ids
        from with_categories
        group by user_id, submitted_at, question_id, answer_id
    )

select
    user_id,
    submitted_at,
    question_id,
    answer_id,
    category_ids,
    subcategory_ids,
    '4' as qpi_version
from v4_with_category_arrays

union all

select
    user_id,
    submitted_at,
    mapped_question_id as question_id,
    mapped_answer_id as answer_id,
    [category_id] as category_ids,
    [subcategory_id] as subcategory_ids,
    'historical' as qpi_version
from {{ ref("int_seed__qpi_answer_historical") }}
