-- Whether at least one table matches a wildcard source (e.g. books_metadata_tmp_*).
-- Not queried at parse time, so that the model keeps its source() lineage.
{% macro tmp_tables_exist(tmp_source) %}
    {% if not execute %} {{ return(true) }} {% endif %}
    {% set query %}
        select count(*) from `{{ tmp_source.database }}.{{ tmp_source.schema }}.INFORMATION_SCHEMA.TABLES`
        where starts_with(table_name, '{{ tmp_source.identifier | replace("*", "") }}')
    {% endset %}
    {{ return(run_query(query).columns[0].values()[0] > 0) }}
{% endmacro %}
