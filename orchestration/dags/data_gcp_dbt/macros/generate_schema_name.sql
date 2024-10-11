{% macro generate_schema_name(custom_schema_name, node=none) -%}

    {%- set default_schema = target.dataset -%}
    {%- set user_name = env_var('USER', 'anonymous_user') -%}
    {%- set is_orchestrated = target.name in ["prod", "stg", "dev"] and target.profile_name != "sandbox" -%}
    {%- set is_elementary = target.profile_name == "elementary" -%}

    {%- if target.profile_name == "CI" or is_elementary -%}
        {{ default_schema }}

     {%- elif target.name == "local" -%}
        {{ "tmp_" ~ user_name ~ "_dev"}}

    {%- elif is_orchestrated -%}
        {{ custom_schema_name | trim if custom_schema_name else (
            node.name.split('__')[0] ~ "_" ~ target.name if 'intermediate' in node.path or 'machine_learning' in node.path
        ) }}

    {%- elif target.profile_name == "sandbox" -%}
        {{ custom_schema_name | trim if custom_schema_name else default_schema }}

    {%- else -%}
        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
