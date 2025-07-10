{%- macro is_first_day_of_month() -%}
    {%- set execution_date = var(
        "execution_date", dbt_airflow_macros.execution_date()
    ) -%}
    {{ print("execution_date: " ~ execution_date) }}
    {%- if execution_date is string -%} {%- set formatted_date = execution_date -%}
    {%- else -%} {%- set formatted_date = execution_date.strftime("%Y-%m-%d") -%}
    {%- endif -%}
    {%- if formatted_date[-2:] == "01" -%}
        {{ print("Macro is_first_day_of_month evalutate to TRUE") }}
        {{ return("TRUE") }}
    {%- else -%}
        {{ print("Macro is_first_day_of_month evalutate to FALSE") }}
        {{ return("FALSE") }}
    {%- endif -%}
{%- endmacro -%}
