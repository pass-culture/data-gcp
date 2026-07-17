with
    offers as (
        select
            co.venue_region_name,
            co.venue_department_name,
            co.venue_department_code,
            co.venue_epci as venue_epci_name,
            co.venue_epci_code,
            co.venue_city as venue_city_name,
            co.venue_city_code,
            date_trunc(
                date(co.collective_offer_creation_date), month
            ) as partition_month,
            coalesce(rd.region_code, -1) as venue_region_code,
            count(co.collective_offer_id) as total_created_collective_offers
        from {{ ref("int_global__collective_offer") }} as co
        left join
            {{ source("seed", "region_department") }} as rd
            on co.venue_department_code = rd.num_dep
        group by
            date_trunc(date(co.collective_offer_creation_date), month),
            co.venue_region_name,
            rd.region_code,
            co.venue_department_name,
            co.venue_department_code,
            co.venue_epci,
            co.venue_epci_code,
            co.venue_city,
            co.venue_city_code
    )

select
    offers.partition_month,
    offers.venue_region_name,
    offers.venue_region_code,
    offers.venue_department_name,
    offers.venue_department_code,
    offers.venue_epci_name,
    offers.venue_epci_code,
    offers.venue_city_name,
    offers.venue_city_code,
    offers.total_created_collective_offers
from offers
