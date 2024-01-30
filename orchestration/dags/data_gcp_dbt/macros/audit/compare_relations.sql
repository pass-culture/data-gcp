
{% macro compare_relations(table_name, legacy_schema_name, primary_key, node, legacy_table_name=none) -%}

    {{ config(warn_if = '>1') }}
    

    {% if not legacy_table_name %}
        {% set legacy_table_name=table_name %}
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
            primary_key=primary_key
        ) }}
    {% endif %}

{%- endmacro %}