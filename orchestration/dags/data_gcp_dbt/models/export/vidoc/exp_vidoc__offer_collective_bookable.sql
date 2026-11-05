-- Bookable (non template) collective offers by cultural partner (venue)
-- geography, as the venue side offer KPIs of
-- external_reporting__eac_offer_booking. Monthly flows. No statistical secret:
-- offer and partner counts are not revenue (external reporting publishes them
-- unmasked at region and academy level, not at department level).
select
    partition_month,
    scholar_year,
    venue_region_name,
    venue_region_code,
    venue_department_name,
    venue_department_code,
    sum(
        total_created_bookable_collective_offers
    ) as total_created_bookable_collective_offers,
    sum(
        total_prereserved_bookable_collective_offers
    ) as total_prereserved_bookable_collective_offers,
    sum(new_bookable_offer_partners) as new_bookable_offer_partners
from {{ ref("metrics_offer__collective_bookable") }}
where
    partition_month <= date_trunc(date_sub(date('{{ ds() }}'), interval 1 month), month)
    and partition_month >= date('2022-01-01')
group by
    partition_month,
    scholar_year,
    venue_region_name,
    venue_region_code,
    venue_department_name,
    venue_department_code
