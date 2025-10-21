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
        updated_at,
        dimension_name,
        dimension_value,
        kpi_name,
        coalesce(numerator, 0) as numerator,
        coalesce(denominator, 0) as denominator,
        coalesce(kpi, 0) as kpi
    from {{ ref("mrt_external_reporting__" ~ model) }}
    where
        1 = 1
        {% if is_incremental() %}
            and partition_month
            = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
        {% else %}
            and partition_month
            <= date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
            and partition_month >= date("2022-01-01")
        {% endif %}
{% endfor %}
