{{ config(materialized="table") }}

{% set question_ids = [
    "SORTIES",
    "FESTIVAL",
    "SPECTACLES",
    "ACTIVITES",
    "PROJECTIONS",
] %}


with
    base as (
        select
            user_id,
            question_id,
            answer_id,
            category_ids,
            subcategory_ids,
            qpi_version,
            submitted_at
        from {{ ref("int_cultural_survey__qpi_internal_survey_v4") }}
    )

select
    user_id,
    qpi_version,
    submitted_at,
    {% for q in question_ids %}
        -- Aggregate answers as array
        array_agg(
            case when question_id = '{{ q }}' then answer_id end ignore nulls
        ) as answers_{{ q }},
        -- Aggregate category_ids arrays, filter nulls in CASE
        array_concat_agg(
            case
                when question_id = '{{ q }}' and category_ids is not null
                then category_ids
                else []
            end
        ) as answers_{{ q }}_category_ids,
        -- Aggregate subcategory_ids arrays, filter nulls in CASE
        array_concat_agg(
            case
                when question_id = '{{ q }}' and subcategory_ids is not null
                then subcategory_ids
                else []
            end
        ) as answers_{{ q }}_subcategory_ids
        {% if not loop.last %},{% endif %}
    {% endfor %}
from base
group by user_id, qpi_version, submitted_at
