{% macro generate_schema_name(custom_schema_name, node=none) -%}

    {%- set default_schema = target.dataset -%}

    {%- if target.profile_name == "CI" or target.name == "local" -%}
        {{ default_schema }}

    {%- elif target.name in ["prod", "stg", "dev"] and target.profile_name != "sandbox" -%}
        {%- if custom_schema_name -%}
            {{ custom_schema_name | trim }}
        {%- elif 'intermediate' in node.path or 'machine_learning' in node.path -%}
            {%- set model_parts = node.name.split('__') -%}
            {{ model_parts[0] ~ "_" ~ target.name }}
        {%- endif -%}

    {%- elif target.profile_name == "sandbox" -%}
        {{ custom_schema_name | trim if custom_schema_name else default_schema }}

    {%- else -%}
        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
