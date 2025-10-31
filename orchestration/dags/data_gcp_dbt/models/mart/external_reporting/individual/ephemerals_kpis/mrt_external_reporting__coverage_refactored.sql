{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_month", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{% set dimensions = [
    {"name": "NAT", "value_expr": "'NAT'"},
    {"name": "REG", "value_expr": "population_region_name"},
    {"name": "DEP", "value_expr": "population_department_name"},
] %}

{% set age_groups = [15, 16, 17, 18] %}

{{ generate_coverage_metrics_by_age(age_groups, dimensions) }}
