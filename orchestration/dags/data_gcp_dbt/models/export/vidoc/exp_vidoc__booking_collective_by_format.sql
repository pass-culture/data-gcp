-- Collective (EAC) bookings by offer format and school geography, as
-- external_reporting__eac_format. Monthly flows. An offer with several formats
-- counts in each of them: formats are not summable. No statistical secret.
select
    partition_month,
    scholar_year,
    collective_offer_format,
    institution_region_name,
    institution_region_code,
    institution_department_name,
    institution_department_code,
    max(institution_academy_name) as institution_academy_name,
    sum(total_collective_bookings) as total_collective_bookings,
    sum(total_collective_booking_amount) as total_collective_booking_amount,
    sum(total_collective_tickets) as total_collective_tickets,
    -- a school sits in one department: distinct counts are summable over
    -- departments
    sum(total_collective_institutions) as total_collective_institutions,
    sum(
        cumulative_total_collective_institutions
    ) as cumulative_total_collective_institutions
from {{ ref("metrics_booking__collective_by_format") }}
where
    collective_offer_format is not null
    and partition_month
    <= date_trunc(date_sub(date('{{ ds() }}'), interval 1 month), month)
    and partition_month >= date('2022-01-01')
group by
    partition_month,
    scholar_year,
    collective_offer_format,
    institution_region_name,
    institution_region_code,
    institution_department_name,
    institution_department_code
