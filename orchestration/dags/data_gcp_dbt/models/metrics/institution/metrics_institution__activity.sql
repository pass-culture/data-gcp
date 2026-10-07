select
    ic.partition_month,
    ic.scholar_year,
    ic.institution_region_name,
    ic.institution_academy_name,
    ic.institution_department_name,
    ic.institution_epci_code,
    ic.institution_city_code,
    ic.total_institutions,
    ic.total_engaged_institutions,
    du.total_deposit,
    du.total_amount_spent,
    du.total_bookings,
    du.cumulative_total_amount_spent,
    du.cumulative_total_bookings
from {{ ref("int_kpi__institution_coverage") }} as ic
left join
    {{ ref("int_kpi__institution_deposit_usage") }} as du
    on ic.partition_month = du.partition_month
    and ic.scholar_year = du.scholar_year
    and ic.institution_region_name = du.institution_region_name
    and ic.institution_academy_name = du.institution_academy_name
    and ic.institution_department_name = du.institution_department_name
    and ic.institution_epci_code = du.institution_epci_code
    and ic.institution_city_code = du.institution_city_code
