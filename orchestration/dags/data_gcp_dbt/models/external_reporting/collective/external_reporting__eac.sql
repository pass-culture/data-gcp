{{
    config(
        materialized="table",
        partition_by={"field": "partition_month", "data_type": "date"},
        cluster_by=["dimension_name", "dimension_value", "kpi_name"],
    )
}}


{#
    aggregation tells consumers how to aggregate numerator and denominator over
    months: "flow" = sum the months, "stock" = take the last month (the value is
    already scholar year to date or a snapshot).

    Rebuilt in full each run: the statistical secret flag is evaluated at the last
    complete month and applied to every month of a secret territory.

    is_secret_applicable selects the KPIs subject to the statistical secret: the
    ones measured on the cultural partner (venue) geography. KPIs measured on the
    school geography (suffixed `_etablissements` in eac_offer_booking) are never
    masked.
#}
{% set models = {
    "eac_coverage": {"aggregation": "stock", "is_secret_applicable": "false"},
    "eac_credit_usage": {
        "aggregation": "stock",
        "is_secret_applicable": "false",
    },
    "eac_cultural_partner": {
        "aggregation": "stock",
        "is_secret_applicable": "true",
    },
    "eac_domain": {"aggregation": "flow", "is_secret_applicable": "false"},
    "eac_format": {"aggregation": "flow", "is_secret_applicable": "false"},
    "eac_offer_booking": {
        "aggregation": "flow",
        "is_secret_applicable": "segment.kpi_name not like '%_etablissements'",
    },
    "eac_institution": {
        "aggregation": "stock",
        "is_secret_applicable": "false",
    },
} %}

with
    kpis as (
        {% for model, segment_config in models.items() %}
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
                '{{ segment_config.aggregation }}' as aggregation,
                {{ segment_config.is_secret_applicable }} as is_secret_applicable
            from {{ ref("external_reporting__" ~ model) }} as segment
            left join
                {{ source("raw", "applicative_database_educational_year") }} as ey
                on segment.partition_month
                between date(ey.educational_year_beginning_date) and date(
                    ey.educational_year_expiration_date
                )
            where
                segment.partition_month
                <= date_trunc(date_sub(date('{{ ds() }}'), interval 1 month), month)
                and segment.partition_month >= date('2022-01-01')
        {% endfor %}
    )

    {{
        apply_statistical_secret(
            kpis_cte="kpis",
            secret_models=["statistical_secret__cultural_partner_collective"],
            kpi_attributes=["aggregation"],
            zero_row_columns={
                "scholar_year": "(select any_value(scholar_year) from kpis where partition_month = date_trunc(date_sub(date('"
                ~ ds()
                ~ "'), interval 1 month), month))"
            },
        )
    }}
