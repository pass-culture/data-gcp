{{
    config(
        materialized="table",
        partition_by={
            "field": "partition_month",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by=["booking_type", "department_code", "epci_code"],
    )
}}

with
    -- an EPCI spanning several departments cannot be subtracted from one of
    -- them, so it never covers municipalities at department or country level
    epci_departments as (
        select
            epci_code, count(distinct department_code) > 1 as is_epci_cross_department
        from {{ ref("int_kpi__cultural_partner_offerer_period_revenue") }}
        where epci_code is not null
        group by epci_code
    ),

    municipality_tree as (
        select distinct
            partition_month, booking_type, municipality_code, epci_code, department_code
        from {{ ref("int_kpi__cultural_partner_offerer_period_revenue") }}
    ),

    -- one row per municipality cell with the hidden status of each level above
    municipality_cells as (
        select
            municipality.partition_month,
            municipality.booking_type,
            municipality.geo_code as municipality_code,
            tree.epci_code,
            tree.department_code,
            municipality.total_revenue_amount,
            municipality.is_primary_secret
            or municipality.is_below_population_floor as is_municipality_hidden,
            coalesce(epci.is_primary_secret, true) as is_epci_hidden,
            coalesce(department.is_primary_secret, true) as is_department_hidden,
            coalesce(country.is_primary_secret, true) as is_country_hidden,
            coalesce(
                epci_departments.is_epci_cross_department, false
            ) as is_epci_cross_department
        from
            {{ ref("int_kpi__cultural_partner_statistical_secret_primary") }}
            as municipality
        inner join
            municipality_tree as tree
            on municipality.partition_month = tree.partition_month
            and municipality.booking_type = tree.booking_type
            and municipality.geo_code = tree.municipality_code
        left join
            {{ ref("int_kpi__cultural_partner_statistical_secret_primary") }} as epci
            on municipality.partition_month = epci.partition_month
            and municipality.booking_type = epci.booking_type
            and epci.geo_level = 'epci'
            and tree.epci_code = epci.geo_code
        left join
            {{ ref("int_kpi__cultural_partner_statistical_secret_primary") }}
            as department
            on municipality.partition_month = department.partition_month
            and municipality.booking_type = department.booking_type
            and department.geo_level = 'department'
            and tree.department_code = department.geo_code
        left join
            {{ ref("int_kpi__cultural_partner_statistical_secret_primary") }} as country
            on municipality.partition_month = country.partition_month
            and municipality.booking_type = country.booking_type
            and country.geo_level = 'country'
        left join epci_departments on tree.epci_code = epci_departments.epci_code
        where municipality.geo_level = 'municipality'
    ),

    -- secondary step 1: municipalities within a published EPCI
    municipality_candidates as (
        select
            partition_month,
            booking_type,
            municipality_code,
            epci_code,
            if(
                is_municipality_hidden,
                0,
                row_number() over (
                    partition by
                        partition_month, booking_type, epci_code, is_municipality_hidden
                    order by total_revenue_amount, municipality_code
                )
            ) as candidate_rank
        from municipality_cells
        where not is_epci_hidden
    ),

    municipality_contributions as (
        select
            window_revenue.partition_month,
            window_revenue.booking_type,
            candidates.epci_code as parent_code,
            candidates.candidate_rank,
            window_revenue.offerer_id,
            window_revenue.revenue_amount
        from
            {{ ref("int_kpi__cultural_partner_offerer_period_revenue") }}
            as window_revenue
        inner join
            municipality_candidates as candidates
            on window_revenue.partition_month = candidates.partition_month
            and window_revenue.booking_type = candidates.booking_type
            and window_revenue.municipality_code = candidates.municipality_code
    ),

    epci_hidden_rank as (
        {{ secondary_suppression_rank("municipality_contributions") }}
    ),

    municipality_step_1 as (
        select
            cells.* except (is_municipality_hidden),
            cells.is_municipality_hidden
            or coalesce(
                candidates.candidate_rank <= ranks.hidden_rank, false
            ) as is_municipality_hidden
        from municipality_cells as cells
        left join
            municipality_candidates as candidates
            on cells.partition_month = candidates.partition_month
            and cells.booking_type = candidates.booking_type
            and cells.municipality_code = candidates.municipality_code
        left join
            epci_hidden_rank as ranks
            on candidates.partition_month = ranks.partition_month
            and candidates.booking_type = ranks.booking_type
            and candidates.epci_code = ranks.parent_code
    ),

    -- secondary step 2: within a published department, the hideable children are
    -- published EPCIs lying in that department only, and published
    -- municipalities outside any EPCI; hiding an EPCI hides its municipalities
    municipality_department_units as (
        select
            *,
            case
                when not is_epci_hidden and not is_epci_cross_department
                then concat('epci:', epci_code)
                when epci_code is null and not is_municipality_hidden
                then concat('municipality:', municipality_code)
            end as unit_code
        from municipality_step_1
        where not is_department_hidden
    ),

    department_unit_candidates as (
        select
            partition_month,
            booking_type,
            department_code,
            unit_code,
            row_number() over (
                partition by partition_month, booking_type, department_code
                order by sum(total_revenue_amount), unit_code
            ) as candidate_rank
        from municipality_department_units
        where unit_code is not null
        group by partition_month, booking_type, department_code, unit_code
    ),

    -- uncovered = hidden municipality not covered by a published EPCI of the
    -- department
    department_unit_contributions as (
        select
            window_revenue.partition_month,
            window_revenue.booking_type,
            municipalities.department_code as parent_code,
            window_revenue.offerer_id,
            window_revenue.revenue_amount,
            coalesce(candidates.candidate_rank, 0) as candidate_rank
        from
            {{ ref("int_kpi__cultural_partner_offerer_period_revenue") }}
            as window_revenue
        inner join
            municipality_department_units as municipalities
            on window_revenue.partition_month = municipalities.partition_month
            and window_revenue.booking_type = municipalities.booking_type
            and window_revenue.municipality_code = municipalities.municipality_code
        left join
            department_unit_candidates as candidates
            on municipalities.partition_month = candidates.partition_month
            and municipalities.booking_type = candidates.booking_type
            and municipalities.department_code = candidates.department_code
            and municipalities.unit_code = candidates.unit_code
        where
            municipalities.unit_code is not null
            or municipalities.is_municipality_hidden
    ),

    department_hidden_rank as (
        {{ secondary_suppression_rank("department_unit_contributions") }}
    ),

    department_unit_secondary as (
        select distinct
            candidates.partition_month, candidates.booking_type, candidates.unit_code
        from department_unit_candidates as candidates
        inner join
            department_hidden_rank as ranks
            on candidates.partition_month = ranks.partition_month
            and candidates.booking_type = ranks.booking_type
            and candidates.department_code = ranks.parent_code
        where candidates.candidate_rank <= ranks.hidden_rank
    ),

    municipality_step_2 as (
        select
            municipalities.* except (is_municipality_hidden, is_epci_hidden),
            municipalities.is_municipality_hidden
            or epci_secondary.unit_code is not null
            or municipality_secondary.unit_code is not null as is_municipality_hidden,
            municipalities.is_epci_hidden
            or epci_secondary.unit_code is not null as is_epci_hidden
        from municipality_step_1 as municipalities
        left join
            department_unit_secondary as epci_secondary
            on municipalities.partition_month = epci_secondary.partition_month
            and municipalities.booking_type = epci_secondary.booking_type
            and concat('epci:', municipalities.epci_code) = epci_secondary.unit_code
        left join
            department_unit_secondary as municipality_secondary
            on municipalities.partition_month = municipality_secondary.partition_month
            and municipalities.booking_type = municipality_secondary.booking_type
            and concat('municipality:', municipalities.municipality_code)
            = municipality_secondary.unit_code
    )

select *
from municipality_step_2
