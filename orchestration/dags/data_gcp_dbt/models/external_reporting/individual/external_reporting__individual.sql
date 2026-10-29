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
#}
{% set models = [
    "beneficiary",
    "coverage",
    "cultural_partner",
    "collective_partner_booking",
    "beneficiary_booking",
    "deposit_usage",
    "diversity",
    "finance",
    "booking",
] %}

with
    kpis as (
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
