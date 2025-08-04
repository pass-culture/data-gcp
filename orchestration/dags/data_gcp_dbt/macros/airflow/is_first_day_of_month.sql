{%- macro is_first_day_of_month() -%}
    {%- set scheduled_date = var(
        "scheduled_date", dbt_airflow_macros.execution_date()
    ) -%}
    {{ print("scheduled_date: " ~ scheduled_date) }}
    {%- if scheduled_date is string -%} {%- set formatted_date = scheduled_date -%}
    {%- else -%} {%- set formatted_date = scheduled_date.strftime("%Y-%m-%d") -%}
    {%- endif -%}
    {%- if formatted_date[-2:] == "01" -%}
        {{ print("Macro is_first_day_of_month evalutate to TRUE") }}
        {{ return("TRUE") }}
    {%- else -%}
        {{ print("Macro is_first_day_of_month evalutate to FALSE") }}
        {{ return("FALSE") }}
    {%- endif -%}
{%- endmacro -%}
