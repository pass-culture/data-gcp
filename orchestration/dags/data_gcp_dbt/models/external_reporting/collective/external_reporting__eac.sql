{{
    config(
        materialized="table",
        partition_by={"field": "partition_month", "data_type": "date"},
        cluster_by=["dimension_name", "dimension_value", "kpi_name"],
    )
}}

{# Statistical secret, zero rows, national values and labels applied to the KPI rows
   of external_reporting__eac_kpis (see that model for the segments). #}
with
    kpis as (select * from {{ ref("external_reporting__eac_kpis") }})

    {{
        apply_statistical_secret(
            kpis_cte="kpis",
            secret_models=["statistical_secret__cultural_partner_collective"],
            kpi_attributes=["aggregation"],
            flow_attribute="aggregation",
            zero_row_columns={
                "scholar_year": "(select any_value(scholar_year) from kpis where partition_month = date_trunc(date_sub(date('"
                ~ ds()
                ~ "'), interval 1 month), month))"
            },
        )
    }}
