{{
    config(
        partition_by={
            "field": "partition_month",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by=["booking_type", "geo_level"],
    )
}}

with
    -- secondary step 3: departments within France
    department_candidates as (
        select
            partition_month,
            booking_type,
            department_code,
            row_number() over (
                partition by partition_month, booking_type
                order by sum(total_revenue_amount), department_code
            ) as candidate_rank
        from {{ ref("int_kpi__cultural_partner_statistical_secret_municipality") }}
        where not is_country_hidden and not is_department_hidden
        group by partition_month, booking_type, department_code
    ),

    -- hiding a department hides all its EPCIs and municipalities
    department_contributions as (
        select
            window_revenue.partition_month,
            window_revenue.booking_type,
            'FR' as parent_code,
            window_revenue.offerer_id,
            window_revenue.revenue_amount,
            if(
                municipalities.is_department_hidden, 0, candidates.candidate_rank
            ) as candidate_rank
        from
            {{ ref("int_kpi__cultural_partner_offerer_window_revenue") }}
            as window_revenue
        inner join
            {{ ref("int_kpi__cultural_partner_statistical_secret_municipality") }}
            as municipalities
            on window_revenue.partition_month = municipalities.partition_month
            and window_revenue.booking_type = municipalities.booking_type
            and window_revenue.municipality_code = municipalities.municipality_code
        left join
            department_candidates as candidates
            on municipalities.partition_month = candidates.partition_month
            and municipalities.booking_type = candidates.booking_type
            and municipalities.department_code = candidates.department_code
        where
            not municipalities.is_country_hidden
            and (
                not municipalities.is_department_hidden
                or (
                    (
                        municipalities.is_epci_hidden
                        or municipalities.is_epci_cross_department
                    )
                    and municipalities.is_municipality_hidden
                )
            )
    ),

    country_hidden_rank as (
        {{ secondary_suppression_rank("department_contributions") }}
    ),

    department_secondary as (
        select
            candidates.partition_month,
            candidates.booking_type,
            candidates.department_code
        from department_candidates as candidates
        inner join
            country_hidden_rank as ranks
            on candidates.partition_month = ranks.partition_month
            and candidates.booking_type = ranks.booking_type
        where candidates.candidate_rank <= ranks.hidden_rank
    ),

    municipality_step_3 as (
        select
            municipalities.* except (
                is_municipality_hidden, is_epci_hidden, is_department_hidden
            ),
            municipalities.is_municipality_hidden
            or secondary.department_code is not null as is_municipality_hidden,
            municipalities.is_epci_hidden
            or secondary.department_code is not null as is_epci_hidden,
            municipalities.is_department_hidden
            or secondary.department_code is not null as is_department_hidden
        from
            {{ ref("int_kpi__cultural_partner_statistical_secret_municipality") }}
            as municipalities
        left join
            department_secondary as secondary
            on municipalities.partition_month = secondary.partition_month
            and municipalities.booking_type = secondary.booking_type
            and municipalities.department_code = secondary.department_code
    ),

    -- an EPCI spanning several departments is hidden if any of its parts is
    final_hidden_cells as (
        select
            partition_month,
            booking_type,
            'municipality' as geo_level,
            municipality_code as geo_code,
            is_municipality_hidden as is_hidden
        from municipality_step_3
        union all
        select
            partition_month,
            booking_type,
            'epci' as geo_level,
            epci_code as geo_code,
            logical_or(is_epci_hidden) as is_hidden
        from municipality_step_3
        where epci_code is not null
        group by partition_month, booking_type, epci_code
        union all
        select
            partition_month,
            booking_type,
            'department' as geo_level,
            department_code as geo_code,
            logical_or(is_department_hidden) as is_hidden
        from municipality_step_3
        where department_code is not null
        group by partition_month, booking_type, department_code
        union all
        select
            partition_month,
            booking_type,
            'country' as geo_level,
            'FR' as geo_code,
            logical_or(is_country_hidden) as is_hidden
        from municipality_step_3
        group by partition_month, booking_type
    ),

    municipality_final as (
        select
            municipalities.partition_month,
            municipalities.booking_type,
            municipalities.municipality_code,
            municipalities.epci_code,
            municipalities.department_code,
            municipalities.is_municipality_hidden,
            municipalities.is_country_hidden,
            municipalities.is_epci_cross_department,
            coalesce(epci.is_hidden, true) as is_epci_hidden,
            coalesce(department.is_hidden, true) as is_department_hidden
        from municipality_step_3 as municipalities
        left join
            final_hidden_cells as epci
            on municipalities.partition_month = epci.partition_month
            and municipalities.booking_type = epci.booking_type
            and epci.geo_level = 'epci'
            and municipalities.epci_code = epci.geo_code
        left join
            final_hidden_cells as department
            on municipalities.partition_month = department.partition_month
            and municipalities.booking_type = department.booking_type
            and department.geo_level = 'department'
            and municipalities.department_code = department.geo_code
    ),

    -- what a reader can deduce by subtracting published cells from a published
    -- parent: it is published as its own cell and must pass the primary rule
    remainder_offerer_cells as (
        select
            window_revenue.partition_month,
            window_revenue.booking_type,
            remainder.geo_level,
            remainder.geo_code,
            window_revenue.offerer_id,
            sum(window_revenue.revenue_amount) as revenue_amount
        from
            {{ ref("int_kpi__cultural_partner_offerer_window_revenue") }}
            as window_revenue
        inner join
            municipality_final as municipalities
            on window_revenue.partition_month = municipalities.partition_month
            and window_revenue.booking_type = municipalities.booking_type
            and window_revenue.municipality_code = municipalities.municipality_code
        cross join
            unnest(
                [
                    struct(
                        'epci_remainder' as geo_level,
                        if(
                            not municipalities.is_epci_hidden
                            and municipalities.is_municipality_hidden,
                            municipalities.epci_code,
                            null
                        ) as geo_code
                    ),
                    struct(
                        'department_remainder',
                        if(
                            not municipalities.is_department_hidden
                            and (
                                municipalities.is_epci_hidden
                                or municipalities.is_epci_cross_department
                            )
                            and municipalities.is_municipality_hidden,
                            municipalities.department_code,
                            null
                        )
                    ),
                    struct(
                        'country_remainder',
                        if(
                            not municipalities.is_country_hidden
                            and municipalities.is_department_hidden
                            and (
                                municipalities.is_epci_hidden
                                or municipalities.is_epci_cross_department
                            )
                            and municipalities.is_municipality_hidden,
                            'FR',
                            null
                        )
                    )
                ]
            ) as remainder
        where remainder.geo_code is not null
        group by all
    ),

    remainder_cells as (
        select
            remainder_offerer_cells.partition_month,
            remainder_offerer_cells.booking_type,
            remainder_offerer_cells.geo_level,
            remainder_offerer_cells.geo_code,
            parent.geo_label,
            parent.population,
            count(*) as total_contributing_offerers,
            sum(remainder_offerer_cells.revenue_amount) as total_revenue_amount,
            safe_divide(
                max(remainder_offerer_cells.revenue_amount),
                sum(remainder_offerer_cells.revenue_amount)
            ) as max_offerer_revenue_share
        from remainder_offerer_cells
        left join
            {{ ref("int_kpi__cultural_partner_statistical_secret_primary") }} as parent
            on remainder_offerer_cells.partition_month = parent.partition_month
            and remainder_offerer_cells.booking_type = parent.booking_type
            and replace(remainder_offerer_cells.geo_level, '_remainder', '')
            = parent.geo_level
            and remainder_offerer_cells.geo_code = parent.geo_code
        group by all
    ),

    all_cells as (
        select
            primary_cells.* except (is_below_population_floor),
            final_hidden_cells.is_hidden as is_statistic_secret,
            primary_cells.is_below_population_floor
        from
            {{ ref("int_kpi__cultural_partner_statistical_secret_primary") }}
            as primary_cells
        inner join
            final_hidden_cells
            on primary_cells.partition_month = final_hidden_cells.partition_month
            and primary_cells.booking_type = final_hidden_cells.booking_type
            and primary_cells.geo_level = final_hidden_cells.geo_level
            and primary_cells.geo_code = final_hidden_cells.geo_code

        union all

        select
            remainder_cells.*,
            {{
                is_statistical_secret(
                    "total_contributing_offerers", "max_offerer_revenue_share"
                )
            }} as is_primary_secret,
            {{
                is_statistical_secret(
                    "total_contributing_offerers", "max_offerer_revenue_share"
                )
            }} as is_statistic_secret,
            false as is_below_population_floor
        from remainder_cells
    )

select
    partition_month,
    booking_type,
    geo_level,
    geo_code,
    geo_label,
    population,
    total_contributing_offerers,
    total_revenue_amount,
    max_offerer_revenue_share,
    is_primary_secret,
    is_statistic_secret,
    case
        when
            total_contributing_offerers
            < {{ var("statistical_secret_min_contributors") }}
            and max_offerer_revenue_share
            >= {{ var("statistical_secret_max_dominance_share") }}
        then 'low_contributors_and_dominance'
        when
            total_contributing_offerers
            < {{ var("statistical_secret_min_contributors") }}
        then 'low_contributors'
        when
            max_offerer_revenue_share
            >= {{ var("statistical_secret_max_dominance_share") }}
        then 'dominance'
        when is_below_population_floor
        then 'population_floor'
        when is_statistic_secret
        then 'secondary'
    end as statistic_secret_reason
from all_cells
