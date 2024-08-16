{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {%- if target.profile_name == "CI" -%}
        {{ node.name }}

    {%- elif custom_alias_name and 'applicative' in node.path -%}
        {{ custom_alias_name ~ node.name }}

    {%- elif ('intermediate' in node.path or 'machine_learning' in node.path or 'backend' in node.path or node.resource_type == 'snapshot') and target.name in ["prod", "stg", "dev"] and target.profile_name != "sandbox" -%}
        {%- set model_name = node.name.split('__')[-1] | trim -%}
        {{ model_name }}

    {%- elif ('mart' in node.path or 'export' in node.path) and target.name in ["prod", "stg", "dev"] and target.profile_name != "sandbox" -%}
        {%- set prefix = 'mrt_' if 'mart' in node.path else 'exp_' -%}
        {%- set model_name = node.name.split('__')[-1] | trim -%}
        {{ node.name.split(prefix)[-1] ~ "_" ~ model_name }}

    {%- elif node.version -%}
        {{ node.name ~ "_v" ~ (node.version | replace(".", "_")) }}

    {%- else -%}
        {{ node.name }}

    {%- endif -%}

{%- endmacro %}
