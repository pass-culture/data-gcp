{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_month", "data_type": "date"},
            on_schema_change="append_new_columns",
            cluster_by=["dimension_name", "dimension_value", "kpi_name"],
        )
    )
}}


{% set models = [
    "beneficiary",
    "coverage",
    "cultural_partner",
    "deposit_usage",
    "diversity",
    "finance",
] %}

{% for model in models %}
    {% if not loop.first %}
        union all
    {% endif %}
    select
        partition_month,
        update_date,
        dimension_name,
        dimension_value,
        kpi_name,
        numerator,
        denominator,
        kpi
    from {{ ref("mrt_external_reporting__ppg_" ~ model) }}
    where
        1 = 1
        {% if is_incremental() %}
            and partition_month = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
        {% endif %}
{% endfor %}
