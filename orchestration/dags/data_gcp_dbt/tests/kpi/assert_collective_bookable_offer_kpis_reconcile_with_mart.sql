-- Summing a scholar year's monthly rows must give the number of bookable offers
-- of that scholar year in the mart, on both geography sides. Returns the scholar
-- years that do not match.
with
    mart as (
        select ey.scholar_year, count(*) as total_created_bookable_collective_offers
        from {{ ref("mrt_global__collective_offer") }} as mco
        inner join
            {{ source("raw", "applicative_database_educational_year") }} as ey
            on mco.collective_stock_beginning_date_time
            between ey.educational_year_beginning_date
            and ey.educational_year_expiration_date
        where not mco.collective_offer_is_template
        group by ey.scholar_year
    ),

    kpis as (
        select
            'venue' as geo_side,
            scholar_year,
            sum(
                total_created_bookable_collective_offers
            ) as total_created_bookable_collective_offers
        from {{ ref("int_kpi__offer_collective_bookable") }}
        group by scholar_year
        union all
        select
            'institution' as geo_side,
            scholar_year,
            sum(
                total_created_bookable_collective_offers
            ) as total_created_bookable_collective_offers
        from {{ ref("int_kpi__offer_collective_bookable_institution") }}
        group by scholar_year
    )

select
    geo_side,
    mart.scholar_year,
    mart.total_created_bookable_collective_offers as mart_total,
    kpis.total_created_bookable_collective_offers as kpi_total
from mart
cross join unnest(['venue', 'institution']) as geo_side
left join kpis on mart.scholar_year = kpis.scholar_year and geo_side = kpis.geo_side
where
    kpis.scholar_year is null
    or kpis.total_created_bookable_collective_offers
    != mart.total_created_bookable_collective_offers
