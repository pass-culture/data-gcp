-- mapping of category_id to question_id
{% set question_mapping = {"-- IGNORE --": "-- IGNORE --"} %}

-- mapping of subcategory_id to answer_id (historical → v4)
{% set answer_mapping = {"-- IGNORE --": "-- IGNORE --"} %}

with
    raw_data as (
        select user_id, submitted_at, category_id, subcategory_id
        from {{ source("raw", "qpi_answers_historical") }}
    )
select
    user_id,
    submitted_at,
    category_id,
    subcategory_id,
    case
        category_id
        {% for old_val, new_val in question_mapping.items() %}
            when '{{ old_val }}' then '{{ new_val }}'
        {% endfor %}
        else null
    end as mapped_question_id,
    case
        subcategory_id
        {% for old_val, new_val in answer_mapping.items() %}
            when '{{ old_val }}' then '{{ new_val }}'
        {% endfor %}
        else null
    end as mapped_answer_id
from raw_data
