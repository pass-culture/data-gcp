{% macro get_age_groups() %}
    {#
        Age groups for coverage analysis.
        Returns: Array of ages [15, 16, 17, 18].
    #}
    {% set age_groups = [15, 16, 17, 18] %} {{ return(age_groups) }}
{% endmacro %}
