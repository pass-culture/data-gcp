{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.dataset -%}
    {%- if custom_schema_name is none or target.name == "dev" or target.profile_name == "sandbox" -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}