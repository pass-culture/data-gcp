with
    booking_source as (
        select
            scholar_year,
            institution_region_name,
            institution_department_name,
            institution_department_code,
            institution_academy_name,
            institution_epci as institution_epci_name,
            institution_epci_code,
            institution_city as institution_city_name,
            institution_city_code,
            booking_amount,
            collective_booking_id,
            date_trunc(date(collective_booking_used_date), month) as partition_month
        from {{ ref("mrt_global__collective_booking") }}
        where
            collective_booking_status = 'REIMBURSED'
            and scholar_year is not null
            and collective_booking_used_date is not null
    ),

    booking_aggregated as (
        select
            partition_month,
            scholar_year,
            institution_region_name,
            institution_academy_name,
            institution_department_name,
            institution_department_code,
            institution_epci_name,
            institution_epci_code,
            institution_city_name,
            institution_city_code,
            sum(booking_amount) as total_amount_spent,
            count(distinct collective_booking_id) as total_bookings
        from booking_source
        group by
            partition_month,
            scholar_year,
            institution_region_name,
            institution_academy_name,
            institution_department_name,
            institution_department_code,
            institution_epci_name,
            institution_epci_code,
            institution_city_name,
            institution_city_code
    ),

    booking_with_cumul as (
        select
            partition_month,
            scholar_year,
            institution_region_name,
            institution_academy_name,
            institution_department_name,
            institution_department_code,
            institution_epci_name,
            institution_epci_code,
            institution_city_name,
            institution_city_code,
            total_amount_spent,
            total_bookings,
            sum(total_amount_spent) over (
                partition by
                    scholar_year,
                    institution_region_name,
                    institution_academy_name,
                    institution_epci_code,
                    institution_city_code
                order by partition_month
                rows unbounded preceding
            ) as cumulative_total_amount_spent,
            sum(total_bookings) over (
                partition by
                    scholar_year,
                    institution_region_name,
                    institution_academy_name,
                    institution_department_code,
                    institution_epci_code,
                    institution_city_code
                order by partition_month
                rows unbounded preceding
            ) as cumulative_total_bookings
        from booking_aggregated
    ),

    budget_source as (
        select
            scholar_year,
            institution_region_name,
            institution_academy_name,
            institution_department_name,
            institution_department_code,
            institution_epci as institution_epci_name,
            institution_epci_code,
            institution_city as institution_city_name,
            institution_city_code,
            total_scholar_year_deposit
        from {{ ref("mrt_collective__eple_aggregated") }}
        where scholar_year is not null
    ),

    budget_aggregated as (
        select
            scholar_year,
            institution_region_name,
            institution_academy_name,
            institution_department_name,
            institution_department_code,
            institution_epci_name,
            institution_epci_code,
            institution_city_name,
            institution_city_code,
            sum(total_scholar_year_deposit) as total_deposit
        from budget_source
        group by
            scholar_year,
            institution_region_name,
            institution_academy_name,
            institution_department_name,
            institution_department_code,
            institution_epci_name,
            institution_epci_code,
            institution_city_name,
            institution_city_code
    ),

    scholar_years_months as (
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

    budget_split_by_month as (
        select
            sm.partition_month,
            b.scholar_year,
            b.institution_region_name,
            b.institution_academy_name,
            b.institution_department_name,
            b.institution_department_code,
            b.institution_epci_name,
            b.institution_epci_code,
            b.institution_city_name,
            b.institution_city_code,
            b.total_deposit
        from budget_aggregated as b
        inner join scholar_years_months as sm on b.scholar_year = sm.scholar_year
    )

select
    b.partition_month,
    b.scholar_year,
    b.institution_region_name,
    b.institution_academy_name,
    b.institution_department_name,
    b.institution_department_code,
    b.institution_epci_name,
    b.institution_epci_code,
    b.institution_city_name,
    b.institution_city_code,
    b.total_deposit,
    coalesce(c.total_amount_spent, 0) as total_amount_spent,
    coalesce(c.total_bookings, 0) as total_bookings,
    coalesce(c.cumulative_total_amount_spent, 0) as cumulative_total_amount_spent,
    coalesce(c.cumulative_total_bookings, 0) as cumulative_total_bookings
from budget_split_by_month as b
left join
    booking_with_cumul as c
    on b.scholar_year = c.scholar_year
    and b.partition_month = c.partition_month
    and b.institution_academy_name = c.institution_academy_name
    and b.institution_department_code = c.institution_department_code
    and b.institution_epci_code = c.institution_epci_code
    and b.institution_city_code = c.institution_city_code
