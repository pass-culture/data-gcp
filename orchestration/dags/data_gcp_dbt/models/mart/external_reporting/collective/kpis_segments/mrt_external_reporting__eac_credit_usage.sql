{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_month", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{% set dimensions = [
    {
        "name": "NAT",
        "value_expr": "dimension_nat",
        "value_filter": "dimension_nat",
    },
    {
        "name": "REG",
        "value_expr": "institution_region_name",
        "value_filter": "venue_region_name",
    },
    {
        "name": "ACAD",
        "value_expr": "institution_academy_name",
        "value_filter": "venue_academy_name",
    },
    {
        "name": "COM",
        "value_expr": "institution_city_code",
        "value_filter": "venue_city_code",
    },
    {
        "name": "EPCI",
        "value_expr": "institution_epci_code",
        "value_filter": "venue_epci_code",
    },
] %}

with
    budget_by_geo as (
        {% for dim in dimensions %}
            select
                '{{ dim.name }}' as dimension_name,
                {% if dim.name == "NAT" %} 'NAT'
                {% else %} {{ dim.value_expr }}
                {% endif %} as dimension_value,
                scholar_year,
                sum(total_scholar_year_deposit) as total_budget_deposit
            from {{ ref("mrt_collective__eple_aggregated") }}
            group by dimension_name, dimension_value, scholar_year
            {% if not loop.last %}
                union all
            {% endif %}
        {% endfor %}
    ),

    all_dimensions as (
        select distinct
            scholar_year,
            'NAT' as dimension_nat,
            institution_region_name,
            institution_academy_name,
            institution_city_code,
            institution_epci_code,
            venue_region_name,
            venue_academy_name,
            venue_city_code,
            venue_epci_code
        from {{ ref("mrt_global__collective_booking") }}
        where collective_booking_status = 'REIMBURSED' and scholar_year is not null
    ),

    all_months as (
        select distinct
            gcb.scholar_year,
            date_trunc(date(gcb.collective_booking_used_date), month) as partition_month
        from {{ ref("mrt_global__collective_booking") }} as gcb
        left join
            {{ source("raw", "applicative_database_educational_year") }} as ey
            on gcb.scholar_year = ey.scholar_year
        where
            gcb.collective_booking_status = 'REIMBURSED'
            and gcb.collective_booking_used_date
            between ey.educational_year_beginning_date
            and ey.educational_year_expiration_date
    ),

    complete_grid as (
        select
            ad.scholar_year,
            ad.dimension_nat,
            ad.institution_region_name,
            ad.institution_academy_name,
            ad.institution_city_code,
            ad.institution_epci_code,
            ad.venue_region_name,
            ad.venue_academy_name,
            ad.venue_city_code,
            ad.venue_epci_code,
            am.partition_month
        from all_dimensions as ad
        inner join all_months as am on ad.scholar_year = am.scholar_year
    ),

    monthly_data as (
        select
            scholar_year,
            institution_region_name,
            institution_academy_name,
            institution_city_code,
            institution_epci_code,
            venue_region_name,
            venue_academy_name,
            venue_city_code,
            venue_epci_code,
            date_trunc(date(collective_booking_used_date), month) as partition_month,
            sum(booking_amount) as total_amount_spent_reimbursed,
            count(distinct collective_booking_id) as total_collective_bookings
        from {{ ref("mrt_global__collective_booking") }}
        where collective_booking_status = 'REIMBURSED'
        group by
            scholar_year,
            institution_region_name,
            institution_academy_name,
            institution_city_code,
            institution_epci_code,
            venue_region_name,
            venue_academy_name,
            venue_city_code,
            venue_epci_code,
            partition_month
    ),

    complete_data as (
        select
            cg.scholar_year,
            cg.dimension_nat,
            cg.institution_region_name,
            cg.institution_academy_name,
            cg.institution_city_code,
            cg.institution_epci_code,
            cg.venue_region_name,
            cg.venue_academy_name,
            cg.venue_city_code,
            cg.venue_epci_code,
            cg.partition_month,
            coalesce(
                md.total_amount_spent_reimbursed, 0
            ) as total_amount_spent_reimbursed,
            coalesce(md.total_collective_bookings, 0) as total_collective_bookings
        from complete_grid as cg
        left join
            monthly_data as md using (
                scholar_year,
                institution_region_name,
                institution_academy_name,
                institution_city_code,
                institution_epci_code,
                venue_region_name,
                venue_academy_name,
                venue_city_code,
                venue_epci_code,
                partition_month
            )
    ),

    booking_cumul_amount as (
        select
            scholar_year,
            dimension_nat,
            institution_region_name,
            institution_academy_name,
            institution_city_code,
            institution_epci_code,
            venue_region_name,
            venue_academy_name,
            venue_city_code,
            venue_epci_code,
            partition_month,
            total_amount_spent_reimbursed,
            total_collective_bookings,
            sum(total_amount_spent_reimbursed) over (
                partition by
                    scholar_year,
                    institution_region_name,
                    institution_academy_name,
                    institution_city_code,
                    institution_epci_code,
                    venue_region_name,
                    venue_academy_name,
                    venue_city_code,
                    venue_epci_code
                order by partition_month
                rows unbounded preceding
            ) as cumulative_amount_spent,
            sum(total_collective_bookings) over (
                partition by
                    scholar_year,
                    institution_region_name,
                    institution_academy_name,
                    institution_city_code,
                    institution_epci_code,
                    venue_region_name,
                    venue_academy_name,
                    venue_city_code,
                    venue_epci_code
                order by partition_month
                rows unbounded preceding
            ) as cumulative_bookings
        from complete_data
    ),

    dim_consumption_agg as (
        {% for dim in dimensions %}
            select
                '{{ dim.name }}' as dimension_name,
                bc.{{ dim.value_expr }} as dimension_value,
                bc.scholar_year,
                bc.partition_month,
                sum(bc.cumulative_amount_spent) as sum_cum_spent,
                sum(bc.cumulative_bookings) as sum_cum_bookings,
                sum(
                    case
                        when bc.{{ dim.value_filter }} = bc.{{ dim.value_expr }}
                        then bc.cumulative_amount_spent
                        else 0
                    end
                ) as sum_cum_spent_local,
                sum(
                    case
                        when bc.{{ dim.value_filter }} = bc.{{ dim.value_expr }}
                        then bc.cumulative_bookings
                        else 0
                    end
                ) as sum_cum_bookings_local
            from booking_cumul_amount as bc
            group by dimension_name, dimension_value, scholar_year, partition_month
            {% if not loop.last %}
                union all
            {% endif %}
        {% endfor %}
    ),

    aggregation_by_scholar_year as (
        {% for dim in dimensions %}
            {% if not loop.first %}
                union all
            {% endif %}

            -- KPI 1 : Taux d'utilisation
            select
                ca.partition_month,
                timestamp("{{ ts() }}") as updated_at,
                '{{ dim.name }}' as dimension_name,
                bg.dimension_value,
                'taux_d_utilisation_du_credit' as kpi_name,
                coalesce(ca.sum_cum_spent, 0) as numerator,
                bg.total_budget_deposit as denominator,
                safe_divide(
                    coalesce(ca.sum_cum_spent, 0), bg.total_budget_deposit
                ) as kpi
            from budget_by_geo as bg
            left join
                dim_consumption_agg as ca
                on bg.dimension_name = ca.dimension_name
                and bg.dimension_value = ca.dimension_value
                and bg.scholar_year = ca.scholar_year
            where
                bg.dimension_name = '{{ dim.name }}'
                {% if is_incremental() %}
                    and ca.partition_month
                    = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
                {% endif %}

            union all

            -- KPI 2 & 3 : Basés sur la consommation
            select
                partition_month,
                timestamp("{{ ts() }}") as updated_at,
                '{{ dim.name }}' as dimension_name,
                dimension_value,
                'montant_moyen_par_reservation' as kpi_name,
                sum_cum_spent as numerator,
                sum_cum_bookings as denominator,
                safe_divide(sum_cum_spent, sum_cum_bookings) as kpi
            from dim_consumption_agg
            where
                dimension_name = '{{ dim.name }}'
                {% if is_incremental() %}
                    and partition_month
                    = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
                {% endif %}

            union all

            select
                partition_month,
                timestamp("{{ ts() }}") as updated_at,
                '{{ dim.name }}' as dimension_name,
                dimension_value,
                'montant_moyen_par_reservation_meme_territoire' as kpi_name,
                sum_cum_spent_local as numerator,
                sum_cum_bookings_local as denominator,
                safe_divide(sum_cum_spent_local, sum_cum_bookings_local) as kpi
            from dim_consumption_agg
            where
                dimension_name = '{{ dim.name }}'
                {% if is_incremental() %}
                    and partition_month
                    = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
                {% endif %}
        {% endfor %}
    )

select
    partition_month,
    updated_at,
    dimension_name,
    dimension_value,
    kpi_name,
    numerator,
    denominator,
    kpi
from aggregation_by_scholar_year
where partition_month is not null and dimension_value is not null
