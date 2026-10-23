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


{#
    aggregation tells consumers how to aggregate numerator and denominator over
    months: "flow" = sum the months, "stock" = take the last month (the value is
    already scholar year to date or a snapshot).
#}
{% set models = {
    "eac_coverage": "stock",
    "eac_credit_usage": "stock",
    "eac_cultural_partner": "stock",
    "eac_domain": "flow",
    "eac_offer_booking": "flow",
    "eac_institution": "stock",
} %}

{% for model, aggregation in models.items() %}
    {% if not loop.first %}
        union all
    {% endif %}
    select
        segment.partition_month,
        segment.updated_at,
        segment.dimension_name,
        segment.dimension_value,
        segment.kpi_name,
        coalesce(segment.numerator, 0) as numerator,
        coalesce(segment.denominator, 0) as denominator,
        coalesce(segment.kpi, 0) as kpi,
        ey.scholar_year,
        '{{ aggregation }}' as aggregation
    from {{ ref("external_reporting__" ~ model) }} as segment
    left join
        {{ source("raw", "applicative_database_educational_year") }} as ey
        on segment.partition_month
        between date(ey.educational_year_beginning_date) and date(
            ey.educational_year_expiration_date
        )
    where
        1 = 1
        {% if is_incremental() %}
            and segment.partition_month
            = date_trunc(date_sub(date('{{ ds() }}'), interval 1 month), month)
        {% else %}
            and segment.partition_month
            <= date_trunc(date_sub(date('{{ ds() }}'), interval 1 month), month)
            and segment.partition_month >= date('2022-01-01')
        {% endif %}
{% endfor %}
