select
    user_id,
    submitted_at,
    question_id,
    answer_id,
    cast(null as string) as category_id,
    cast(null as string) as subcategory_id,
    '4' as qpi_version
from {{ ref("int_pcapi__raw_qpi_answer_v4") }}
union all
select
    user_id,
    submitted_at,
    mapped_question_id as question_id,
    mapped_answer_id as answer_id,
    category_id,
    subcategory_id,
    'historical' as qpi_version
from {{ ref("int_seed__qpi_answer_historical") }}
