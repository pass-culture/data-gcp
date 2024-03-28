{% macro generate_schema_name(custom_schema_name, node=none) -%}

    {%- set default_schema = target.dataset -%}

    {%- if 'intermediate' in node.path -%}
        {%- set model_parts = node.name.split('__') -%}
         {%- set schema_name = model_parts[0] ~ "_" ~ target.name -%}
            {{ schema_name }}

    {%- elif custom_schema_name is none or target.name == "dev" or target.profile_name == "sandbox" -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}