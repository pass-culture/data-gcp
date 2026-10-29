{{
    config(
        materialized="table",
        partition_by={"field": "partition_month", "data_type": "date"},
        cluster_by=["dimension_name", "dimension_value", "kpi_name"],
    )
}}

{# Statistical secret, zero rows, national values and labels applied to the KPI rows
   of external_reporting__individual_kpis (see that model for the segments). #}
with
    kpis as (select * from {{ ref("external_reporting__individual_kpis") }})

    {{
        apply_statistical_secret(
            kpis_cte="kpis",
            secret_models=[
                "statistical_secret__cultural_partner_individual",
                "statistical_secret__cultural_partner_collective",
            ],
            low_count_models=["external_reporting__beneficiary_booking_bookers"],
            low_count_denominator_kpis=[
                "montant_moyen_",
                "pct_beneficiaires_ayant_reserve_",
            ],
            kpi_attributes=["aggregation"],
            flow_attribute="aggregation",
        )
    }}
