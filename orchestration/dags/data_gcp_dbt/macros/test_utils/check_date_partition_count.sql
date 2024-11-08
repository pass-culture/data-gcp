{% macro check_date_partition_count(model, date_column_name, where_condition=null) %}
    {% if execute %}
        {% set database_name = target.project %}
        {% set schema_name = model.schema %}
        {% set table_name = model.identifier %}

        {% if where_condition %}
            {% set adjusted_where_condition = where_condition.replace(
                date_column_name, "partition_date"
            ) %}
        {% else %} {% set adjusted_where_condition = None %}
        {% endif %}

        {% set query %}
            WITH partitions AS (
                SELECT
                    SAFE.PARSE_DATE('%Y%m%d', partition_id) AS partition_date
                FROM `{{ database_name }}.{{ schema_name }}.INFORMATION_SCHEMA.PARTITIONS`
                WHERE table_name = '{{ table_name }}'
                    AND SAFE.PARSE_DATE('%Y%m%d', partition_id) IS NOT NULL
            ),
            date_span AS (
                SELECT
                    MIN(partition_date) AS min_date,
                    MAX(partition_date) AS max_date,
                    COUNT(DISTINCT partition_date) AS partition_cnt
                FROM partitions
                {% if where_condition %}
                    where {{ adjusted_where_condition }}
                {% endif %}
            ),
            missing_count AS (
                SELECT
                    COALESCE(DATE_DIFF(max_date, min_date, DAY) + 1 - partition_cnt, 0) AS missing_partition_count
                FROM date_span
            )
            SELECT missing_partition_count FROM missing_count
        {% endset %}

        {% set result = run_query(query) %}

        {% if result is not none and result.rows | length > 0 %}
            {% set missing_partition_count = result.rows[0][
                "missing_partition_count"
            ] %}
            {% if missing_partition_count | int > 0 %} {{ return(true) }}
            {% else %} {{ return(false) }}
            {% endif %}
        {% else %} {{ return(false) }}
        {% endif %}
    {% else %} {{ return(false) }}
    {% endif %}
{% endmacro %}
