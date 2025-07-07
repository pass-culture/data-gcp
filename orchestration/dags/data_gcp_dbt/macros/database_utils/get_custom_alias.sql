{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {%- set is_orchestrated = (
        target.name in ["prod", "stg", "dev"]
        and target.profile_name != "sandbox"
    ) -%}
    {%- set is_elementary = target.profile_name == "elementary" -%}
    {%- set is_ci = target.profile_name == "CI" -%}
    {%- set is_local = target.name == "local" -%}
    {%- set is_applicative = "applicative" in node.path -%}
    {%- set is_intermediate_or_ml = (
        "intermediate" in node.path
        or "machine_learning" in node.path
        or "backend" in node.path
        or node.resource_type == "snapshot"
    ) -%}
    {%- set is_mart_or_export = "mart" in node.path or "export" in node.path -%}
    {%- set is_raw_snapshot = (
        "raw_applicative" in node.path and node.resource_type == "snapshot"
    ) -%}
    {%- set is_raw_applicative = "raw_applicative" in node.name -%}
    {%- set is_mart_or_export = "mart" in node.path or "export" in node.path -%}

    {%- if is_ci or is_local or is_elementary -%} {{ node.name }}

    {%- elif is_raw_snapshot or is_raw_applicative -%}
        {{ "applicative_database_" ~ node.name.split("__")[-1] | trim }}

    {%- elif is_applicative and custom_alias_name -%}
        {{ custom_alias_name ~ node.name }}

    {%- elif is_intermediate_or_ml and is_orchestrated -%}
        {{ node.name.split("__")[-1] | trim }}

    {%- elif is_mart_or_export and is_orchestrated -%}
        {%- set prefix = "mrt_" if "mart" in node.path else "exp_" -%}
        {%- set model_name_parts = node.name.split("__") -%}
        {%- set model_folder = model_name_parts[0].split(prefix)[-1] | trim -%}
        {{ model_folder ~ "_" ~ model_name_parts[-1] | trim }}

    {%- elif node.version -%}
        {{ node.name ~ "_v" ~ (node.version | replace(".", "_")) }}

    {%- else -%} {{ node.name }}

    {%- endif -%}

{%- endmacro %}
