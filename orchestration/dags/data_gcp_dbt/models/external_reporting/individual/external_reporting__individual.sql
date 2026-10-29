{{
    config(
        materialized="table",
        partition_by={"field": "partition_month", "data_type": "date"},
        cluster_by=["dimension_name", "dimension_value", "kpi_name"],
    )
}}

{#
    Rebuilt in full each run: the statistical secret flag is evaluated at the last
    complete month and applied to every month of a secret territory.
    A territory is secret if it is secret on the individual or the collective side
    (cultural partner pages show both).

    Each segment maps to the KPIs subject to the statistical secret: the ones
    measured on the cultural partner (venue) geography. KPIs measured on the
    beneficiary geography are never masked.
#}
{% set models = {
    "beneficiary": "false",
    "coverage": "false",
    "cultural_partner": "true",
    "collective_partner_booking": "true",
    "beneficiary_booking": "false",
    "deposit_usage": "false",
    "diversity": "false",
    "finance": "true",
    "booking": "kpi_name not like 'part_partenaires_dans_conso_beneficiaires%'",
} %}

with
    kpis as (
        {% for model, is_secret_applicable in models.items() %}
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
                coalesce(kpi, 0) as kpi,
                {{ is_secret_applicable }} as is_secret_applicable
            from {{ ref("external_reporting__" ~ model) }}
            where
                partition_month
                <= date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
                and partition_month >= date("2022-01-01")
        {% endfor %}
    )

    {{
        apply_statistical_secret(
            kpis_cte="kpis",
            secret_models=[
                "statistical_secret__cultural_partner_individual",
                "statistical_secret__cultural_partner_collective",
            ],
        )
    }}
