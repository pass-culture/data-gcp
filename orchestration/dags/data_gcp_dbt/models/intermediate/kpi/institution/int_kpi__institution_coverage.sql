-- depends_on: {{ ref('mrt_global__educational_deposit') }}
with
    static_months as (
        select cast(date_month as date) as month_date
        from
            (
                {{
                    dbt_utils.date_spine(
                        datepart="month",
                        start_date="cast('2021-01-01' as date)",
                        end_date="current_date()",
                    )
                }}
            )
    ),

    year_mapping as (
        select
            educational_year_id,
            scholar_year,
            educational_year_beginning_date,
            educational_year_expiration_date
        from {{ source("raw", "applicative_database_educational_year") }}
    ),

    months_spine as (
        select m.month_date as partition_month, y.educational_year_id, y.scholar_year
        from year_mapping as y
        inner join
            static_months as m
            on m.month_date
            between y.educational_year_beginning_date
            and y.educational_year_expiration_date
    ),

    institution_monthly_base as (
        select
            ms.partition_month,
            ms.scholar_year,
            dep.institution_id,
            inst.institution_region_name,
            inst.institution_academy_name,
            inst.institution_department_name,
            inst.institution_epci_code,
            inst.institution_city_code
        from {{ ref("mrt_global__educational_deposit") }} as dep
        inner join
            months_spine as ms on dep.educational_year_id = ms.educational_year_id
        left join
            {{ ref("mrt_global__educational_institution") }} as inst
            on dep.institution_id = inst.institution_id
    ),

    first_bookings as (
        select
            educational_institution_id as institution_id,
            scholar_year,
            min(
                date_trunc(collective_booking_creation_date, month)
            ) as first_booking_month
        from {{ ref("mrt_global__collective_booking") }}
        where collective_booking_status != 'CANCELLED'
        group by institution_id, scholar_year
    )

select
    base.partition_month,
    base.scholar_year,
    base.institution_region_name,
    base.institution_academy_name,
    base.institution_department_name,
    base.institution_epci_code,
    base.institution_city_code,
    coalesce(count(distinct base.institution_id), 0) as total_institutions,
    coalesce(
        count(
            distinct case
                when fb.first_booking_month <= base.partition_month
                then base.institution_id
            end
        ),
        0
    ) as total_engaged_institutions
from institution_monthly_base as base
left join
    first_bookings as fb
    on base.institution_id = fb.institution_id
    and base.scholar_year = fb.scholar_year
group by
    base.partition_month,
    base.scholar_year,
    base.institution_region_name,
    base.institution_academy_name,
    base.institution_department_name,
    base.institution_epci_code,
    base.institution_city_code
