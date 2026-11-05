with
    free_offers as (
        select
            venue_region_name,
            venue_department_name,
            venue_epci_code,
            venue_municipality_code,
            date_trunc(date(offer_creation_date), month) as partition_month,
            count(distinct offer_id) as total_created_free_offers,
            count(
                distinct if(offer_is_bookable, offer_id, null)
            ) as total_created_free_bookable_offers
        from {{ ref("mrt_global__offer") }}
        where last_stock_price = 0
        group by
            partition_month,
            venue_region_name,
            venue_department_name,
            venue_epci_code,
            venue_municipality_code
    ),

    -- every month from the first free offer of the territory to the current
    -- month, so that the cumulative stock exists every month and sums across
    -- territories
    territory_months as (
        select
            territories.venue_region_name,
            territories.venue_department_name,
            territories.venue_epci_code,
            territories.venue_municipality_code,
            partition_month
        from
            (
                select
                    venue_region_name,
                    venue_department_name,
                    venue_epci_code,
                    venue_municipality_code,
                    min(partition_month) as first_month
                from free_offers
                group by
                    venue_region_name,
                    venue_department_name,
                    venue_epci_code,
                    venue_municipality_code
            ) as territories
        cross join
            unnest(
                generate_date_array(
                    territories.first_month,
                    date_trunc(current_date(), month),
                    interval 1 month
                )
            ) as partition_month
    )

select
    tm.partition_month,
    tm.venue_region_name,
    tm.venue_department_name,
    tm.venue_epci_code,
    tm.venue_municipality_code,
    coalesce(fo.total_created_free_offers, 0) as total_created_free_offers,
    coalesce(
        fo.total_created_free_bookable_offers, 0
    ) as total_created_free_bookable_offers,
    sum(coalesce(fo.total_created_free_offers, 0)) over w
    as cumulative_total_created_free_offers,
    sum(coalesce(fo.total_created_free_bookable_offers, 0)) over w
    as cumulative_total_created_free_bookable_offers
from territory_months as tm
left join
    free_offers as fo
    on tm.partition_month = fo.partition_month
    and tm.venue_region_name is not distinct from fo.venue_region_name
    and tm.venue_department_name is not distinct from fo.venue_department_name
    and tm.venue_epci_code is not distinct from fo.venue_epci_code
    and tm.venue_municipality_code is not distinct from fo.venue_municipality_code
window
    w as (
        partition by
            tm.venue_region_name,
            tm.venue_department_name,
            tm.venue_epci_code,
            tm.venue_municipality_code
        order by tm.partition_month
        rows unbounded preceding
    )
