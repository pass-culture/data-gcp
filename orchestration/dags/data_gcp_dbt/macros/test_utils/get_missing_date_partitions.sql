{% macro get_missing_date_partitions(model, date_column_name, where_condition=null) %}
    {% set has_missing_partitions = check_date_partition_count(
        model,
        date_column_name,
        where_condition=where_condition,
    ) %}

    {% if has_missing_partitions and execute %}

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
                    MAX(partition_date) AS max_date
                FROM partitions
                {% if where_condition %}
                    where {{ adjusted_where_condition }}
                {% endif %}
            ),
            all_dates AS (
                SELECT
                    date AS date
                FROM
                    date_span,
                    UNNEST(GENERATE_DATE_ARRAY(date_span.min_date, date_span.max_date, INTERVAL 1 DAY)) AS date
                WHERE date_span.min_date IS NOT NULL AND date_span.max_date IS NOT NULL
            ),
            existing_partitions AS (
                SELECT DISTINCT partition_date AS date
                FROM partitions
            ),
            missing_partitions AS (
                SELECT
                    all_dates.date AS missing_partition_date
                FROM
                    all_dates
                LEFT JOIN
                    existing_partitions
                ON all_dates.date = existing_partitions.date
                WHERE
                    existing_partitions.date IS NULL
            )
            SELECT missing_partition_date FROM missing_partitions
        {% endset %}

        {% set result = run_query(query) %}
        {% set output = [] %}

        {% if result is not none and result.rows | length > 0 %}
            {% for row in result.rows %}
                {% do output.append(row["missing_partition_date"]) %}
            {% endfor %}
        {% endif %}

        {{ return(output) }}
    {% else %} {{ return([]) }}
    {% endif %}
{% endmacro %}
