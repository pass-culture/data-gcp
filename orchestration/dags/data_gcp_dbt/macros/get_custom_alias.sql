{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {%- if target.profile_name == "CI" or target.name == "local" -%}
        {{ node.name }}

    {%- elif custom_alias_name and 'applicative' in node.path -%}
        {{ custom_alias_name ~ node.name }}

    {%- elif ('intermediate' in node.path or 'machine_learning' in node.path or 'backend' in node.path or node.resource_type == 'snapshot') and target.name in ["prod", "stg", "dev"] and target.profile_name != "sandbox" -%}
        {%- set model_name = node.name.split('__') -%}
        {{ model_name[-1] | trim }}

    {%- elif ('mart' in node.path or 'export' in node.path) and target.name in ["prod", "stg", "dev"] and target.profile_name != "sandbox" -%}
        {%- set prefix = 'mrt_' if 'mart' in node.path else 'exp_' -%}
        {%- set model_name_parts = node.name.split('__') -%}
        {%- set model_folder = model_name_parts[0].split(prefix)[-1] | trim -%}
        {%- set model_name = model_name_parts[-1] | trim -%}
        {{ model_folder ~ "_" ~ model_name }}

    {%- elif node.version -%}
        {{ node.name ~ "_v" ~ (node.version | replace(".", "_")) }}

    {%- else -%}
        {{ node.name }}

    {%- endif -%}

{%- endmacro %}
