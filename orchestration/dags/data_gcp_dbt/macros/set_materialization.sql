{% macro set_materialization(non_ci_materialization='table') %}
    {%- if target.name == 'CI' -%}
        {{ return('view') }}
    {%- else -%}
        {{ return(non_ci_materialization='table') }}
    {%- endif -%}
{% endmacro %}
