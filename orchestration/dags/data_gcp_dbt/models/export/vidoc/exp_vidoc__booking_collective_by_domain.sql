-- Collective (EAC) bookings by educational domain and school geography, as
-- external_reporting__eac_domain. Monthly flows. An offer with several domains
-- counts in each of them: domains are not summable. No statistical secret.
select
    partition_month,
    scholar_year,
    educational_domain_name,
    institution_region_name,
    institution_region_code,
    institution_department_name,
    institution_department_code,
    max(institution_academy_name) as institution_academy_name,
    sum(total_collective_bookings) as total_collective_bookings,
    sum(total_collective_booking_amount) as total_collective_booking_amount,
    sum(total_collective_tickets) as total_collective_tickets,
    sum(total_labelled_collective_bookings) as total_labelled_collective_bookings,
    -- a school sits in one department: distinct counts are summable over
    -- departments
    sum(total_collective_institutions) as total_collective_institutions,
    sum(
        cumulative_total_collective_institutions
    ) as cumulative_total_collective_institutions
from {{ ref("metrics_booking__collective_by_domain") }}
where
    educational_domain_name in (
        {% for domain in get_domains() %}
            '{{ domain.label }}'{% if not loop.last %},{% endif %}
        {% endfor %}
    )
    and partition_month
    <= date_trunc(date_sub(date('{{ ds() }}'), interval 1 month), month)
    and partition_month >= date('2022-01-01')
group by
    partition_month,
    scholar_year,
    educational_domain_name,
    institution_region_name,
    institution_region_code,
    institution_department_name,
    institution_department_code
