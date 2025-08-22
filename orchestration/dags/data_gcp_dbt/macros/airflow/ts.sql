{%- macro ts() -%}
    {%- set execution_date = var("execution_date", dbt_airflow_macros.ts()) -%}
    {% if execution_date is string %} {{- execution_date -}}
    {% else %} {{- execution_date.strftime("%Y-%m-%dT%H:%M:%S+00:00") -}}
    {% endif %}
{%- endmacro -%}
