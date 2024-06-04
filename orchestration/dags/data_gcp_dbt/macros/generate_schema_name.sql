{% macro generate_schema_name(custom_schema_name, node=none) -%}

    {%- set default_schema = target.dataset -%}
    {%- if target.profile_name == "CI" -%}
        {{ default_schema }}
    {%- elif custom_schema_name and  ((target.name == "prod" and target.profile_name != "sandbox") or target.name == "stg") -%}
    
        {{ custom_schema_name | trim }}

    {%- elif 'intermediate' in node.path and ((target.name == "prod" and target.profile_name != "sandbox") or target.name == "stg") -%}
        {%- set model_parts = node.name.split('__') -%}
         {%- set schema_name = model_parts[0] ~ "_" ~ target.name -%}
            {{ schema_name }}

    {%- elif 'machine_learning' in node.path and ((target.name == "prod" and target.profile_name != "sandbox") or target.name == "stg") -%}
        {%- set model_parts = node.name.split('__') -%}
            {%- set schema_name = model_parts[0] ~ "_" ~ target.name -%}
                {{ schema_name }}

    {%- elif custom_schema_name is none or target.name == "dev" or target.profile_name == "sandbox" -%}

        {{ default_schema }}

    {%- else -%}

    {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}