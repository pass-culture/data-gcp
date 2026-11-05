-- Collective (EAC) bookings by school geography, as the `_etablissements`
-- booking KPIs of external_reporting__eac_offer_booking. Monthly flows. No
-- statistical secret (school geography).
select
    partition_month,
    scholar_year,
    institution_region_name,
    institution_region_code,
    institution_department_name,
    institution_department_code,
    max(institution_academy_name) as institution_academy_name,
    sum(total_collective_bookings) as total_collective_bookings,
    sum(total_collective_amount_spent) as total_collective_amount_spent
from {{ ref("metrics_booking__collective_institution") }}
where
    partition_month <= date_trunc(date_sub(date('{{ ds() }}'), interval 1 month), month)
    and partition_month >= date('2022-01-01')
group by
    partition_month,
    scholar_year,
    institution_region_name,
    institution_region_code,
    institution_department_name,
    institution_department_code
