{% macro generate_export_tables(
    export_tables,
    secret_partner_value,
    export_schema,
    export_schema_expiration_day=1,
    fields_obfuscation_config={"string_field_dummy_example": "upper({})"}
) %}
    -- Log the target schema and table
    {{ log("Creating schema if not exists: " ~ export_schema, info=True) }}

    {% if execute %}
        -- Create the schema if it doesn't exist
        {% do run_query(
            "CREATE SCHEMA IF NOT EXISTS `"
            ~ target.database
            ~ "."
            ~ export_schema
            ~ "`;"
        ) %}

        {% do run_query(
            "ALTER SCHEMA `"
            ~ target.database
            ~ "."
            ~ export_schema
            ~ "` SET OPTIONS(default_table_expiration_days = "
            ~ export_schema_expiration_day
            ~ ");"
        ) %}

        -- Loop over the export tables and execute SQL for each
        {% for table in export_tables %}
            {{ log("Obfuscating table: " ~ table, info=True) }}
            {% set columns = adapter.get_columns_in_relation(ref(table)) %}
            {% set id_columns = [] %}
            {% set wm_columns = [] %}
            {% for col in columns %}
                {% if col.name.endswith("_id") %} {% do id_columns.append(col.name) %}
                {% elif col.name in fields_obfuscation_config.keys() %}
                    {% do wm_columns.append(col.name) %}
                {% endif %}
            {% endfor %}
            {{ log("Salting id fields :" ~ id_columns, info=True) }}
            {{ log("Watermarking fields :" ~ wm_columns, info=True) }}
            {% set table_sql %}
                CREATE OR REPLACE TABLE {{ target.database }}.{{ export_schema }}.{{ table }} AS
                SELECT
                    {% for col in columns %}
                        {% if col.name.endswith('_id') %}
                            {{ obfuscate_id(col.name, secret_partner_value) }} AS {{ col.name }}
                        {% else %}
                            {{ watermark_field(col.name, fields_obfuscation_config) }} AS {{ col.name }}
                        {% endif %}
                        {% if not loop.last %},{% endif %}
                    {% endfor %}

                FROM {{ ref(table) }};
            {% endset %}

            -- Execute the SQL for the current table
            {% do run_query(table_sql) %}
            {{
                log(
                    "Table created at: "
                    ~ target.database
                    ~ "."
                    ~ export_schema
                    ~ "."
                    ~ table,
                    info=True,
                )
            }}
        {% endfor %}
    {% else %}
        {{
            log(
                "This is a dry run or compilation step. No SQL will be executed.",
                info=True,
            )
        }}
    {% endif %}

    {{ log("Export loop completed.", info=True) }}
{% endmacro %}
