{% macro plausible_beneficiary_age(age_column, month_column) %}
    {#-
        Age, or null when no credit history allows it (inconsistent declared birth
        dates). The age is measured on the first day of the month, so a young
        person credited at 15 during the month is 14.
        Youngest: 14 until the March 2025 reform (no credit at 15 since), then one
        more year per year. Oldest: 19 in February 2019 (credit at 18, open until
        the 19th birthday), then one more year per year.
    -#}
    if(
        {{ age_column }} between if(
            {{ month_column }} < "2025-03-01",
            14,
            14 + div(date_diff({{ month_column }}, date("2025-02-01"), month) + 11, 12)
        ) and 19
        + div(date_diff({{ month_column }}, date("2019-02-01"), month), 12),
        {{ age_column }},
        null
    )
{% endmacro %}
