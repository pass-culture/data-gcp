{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_month", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{% set dimensions = get_dimensions("population", "geo") %}
{% set age_groups = get_age_groups() %}

{{ generate_coverage_metrics_by_age(age_groups, dimensions) }}
