{% macro calculate_exact_age(reference_date, birth_date) %}
    date_diff({{ reference_date }}, {{ birth_date }}, year) - if(
        extract(month from {{ birth_date }}) * 100 + extract(day from {{ birth_date }})
        > extract(month from {{ reference_date }}) * 100
        + extract(day from {{ reference_date }}),
        1,
        0
    )
{% endmacro %}
