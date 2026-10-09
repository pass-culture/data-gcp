{{
    config(
        materialized="table",
        partition_by={
            "field": "partition_month",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by=["booking_type", "geo_level"],
    )
}}

with
    geo_labels as (
        select
            level.geo_level,
            level.geo_code,
            any_value(level.geo_label) as geo_label,
            sum(socio.total_population) as population
        from {{ ref("int_seed__geo_iris") }} as geo
        left join
            {{ ref("int_seed__geo_iris_socio_demographics") }} as socio
            on geo.iris_code = socio.iris_code
        cross join
            unnest(
                [
                    struct(
                        'country' as geo_level, 'FR' as geo_code, 'France' as geo_label
                    ),
                    struct('department', geo.department_code, geo.department_name),
                    struct('epci', geo.epci_code, geo.epci_label),
                    struct(
                        'municipality', geo.municipality_code, geo.municipality_label
                    )
                ]
            ) as level
        where level.geo_code is not null
        group by level.geo_level, level.geo_code
    ),

    offerer_cells as (
        select
            window_revenue.partition_month,
            window_revenue.booking_type,
            level.geo_level,
            level.geo_code,
            window_revenue.offerer_id,
            sum(window_revenue.revenue_amount) as revenue_amount
        from
            {{ ref("int_kpi__cultural_partner_offerer_window_revenue") }}
            as window_revenue
        cross join
            unnest(
                [
                    struct('country' as geo_level, 'FR' as geo_code),
                    struct('department', window_revenue.department_code),
                    struct('epci', window_revenue.epci_code),
                    struct('municipality', window_revenue.municipality_code)
                ]
            ) as level
        where level.geo_code is not null
        group by all
    ),

    primary_cells as (
        select
            offerer_cells.partition_month,
            offerer_cells.booking_type,
            offerer_cells.geo_level,
            offerer_cells.geo_code,
            geo_labels.geo_label,
            geo_labels.population,
            count(*) as total_contributing_offerers,
            sum(offerer_cells.revenue_amount) as total_revenue_amount,
            safe_divide(
                max(offerer_cells.revenue_amount), sum(offerer_cells.revenue_amount)
            ) as max_offerer_revenue_share
        from offerer_cells
        left join
            geo_labels
            on offerer_cells.geo_level = geo_labels.geo_level
            and offerer_cells.geo_code = geo_labels.geo_code
        group by all
    )

select
    *,
    {{
        is_statistical_secret(
            "total_contributing_offerers", "max_offerer_revenue_share"
        )
    }} as is_primary_secret,
    geo_level = 'municipality'
    and coalesce(population, 0)
    < {{ var("statistical_secret_min_municipality_population") }}
    as is_below_population_floor
from primary_cells
