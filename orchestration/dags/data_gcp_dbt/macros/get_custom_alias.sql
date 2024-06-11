{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {%- if custom_alias_name and 'applicative' in node.path -%}

        {{ custom_alias_name ~ node.name }}

    {%- elif custom_alias_name -%}

        {{ custom_alias_name }}

    {%- elif target.profile_name == "CI" -%}

        {{ node.name }}

    {%- elif ('intermediate' in node.path or 'machine_learning' in node.path) and ((target.name == "prod" and target.profile_name != "sandbox") or target.name == "stg")  -%}

        {%- set model_name = node.name.split('__') -%}
        {{ model_name[-1] | trim }}

    {%- elif ('mart' in node.path or 'export' in node.path) and ((target.name == "prod" and target.profile_name != "sandbox") or target.name == "stg") -%}
        {%- set prefix = 'mrt_' if 'mart' in node.path else 'exp_' %}
        {%- set model_name = node.name.split('__') -%}
        {%- set model_name_parts = model_name[0].split(prefix) -%}
        {%- set model_name = model_name_parts[-1] | trim ~ "_" ~ model_name[-1] | trim -%}
            {{ model_name }}

    {%- elif node.version -%}

        {{ return(node.name ~ "_v" ~ (node.version | replace(".", "_"))) }}

    {%- else -%}

        {{ node.name }}

    {%- endif -%}

{%- endmacro %}
