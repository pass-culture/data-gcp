{%- macro ds() -%}
    {%- set execution_date = var('execution_date', dbt_airflow_macros.execution_date()) -%}
    {% if execution_date is string %}
        {{- execution_date -}}
    {% else %}
        {{- execution_date.strftime("%Y-%m-%d") -}}
    {% endif %}
{%- endmacro -%}
