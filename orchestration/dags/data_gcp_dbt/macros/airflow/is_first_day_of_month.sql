{%- macro is_first_day_of_month() -%}
    {%- set execution_date = var(
        "execution_date", dbt_airflow_macros.execution_date()
    ) -%}

    {%- if execution_date is string -%}
        {%- set formatted_date = execution_date -%}
    {%- else -%}
        {%- set formatted_date = execution_date.strftime("%Y-%m-%d") -%}
    {%- endif -%}
    {{ print("Execution date: " ~ formatted_date) }}
    {%- if formatted_date[-2:] == "01" -%} {{ return("TRUE") }}
    {%- else -%}  {{ return("FALSE") }}
    {%- endif -%}
{%- endmacro -%}
