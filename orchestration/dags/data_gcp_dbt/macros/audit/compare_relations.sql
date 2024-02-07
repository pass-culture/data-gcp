
{% macro compare_relations(table_name, legacy_schema_name, primary_key, node, legacy_table_name=none, exclude_columns=[]) -%}

    {{ config(warn_if = '>= 1') }}
    
    {% if not legacy_table_name %}
        {% set legacy_table_name=table_name %}
    {% endif %}

    {% if table_name in ["educational_institution", "offer", "venue"] %}
        {% set legacy_table_name="applicative_database_" ~ table_name %}
    {% endif %}

    {% if execute %}
        {% set legacy_relation=adapter.get_relation(
            database=target.database,
            schema=legacy_schema_name ~ '_' ~ target.name,
            identifier=legacy_table_name
        ) -%}

        {% set dbt_relation=ref(table_name) %}

        {{ audit_helper.compare_relations(
            a_relation=legacy_relation,
            b_relation=dbt_relation,
            summarize=false,
            exclude_columns=exclude_columns,
            primary_key=primary_key
        ) }}
    {% endif %}

{%- endmacro %}