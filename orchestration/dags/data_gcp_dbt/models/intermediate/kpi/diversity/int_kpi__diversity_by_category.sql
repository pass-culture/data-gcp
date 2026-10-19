select
    oc.user_is_in_qpv as is_in_qpv,
    oc.user_macro_density_label as macro_density_label,
    oc.user_density_label as micro_density_label,
    oc.user_region_name as region_name,
    rd.region_code,
    oc.user_department_name as department_name,
    oc.user_department_code as department_code,
    oc.user_epci as epci_name,
    oc.user_epci_code as epci_code,
    oc.user_city as city_name,
    oc.user_city_code as city_code,
    oc.user_municipality_label as municipality_label,
    oc.user_municipality_code as municipality_code,
    cat.offer_category_id,
    date_trunc(date(oc.user_expiration_month), month) as deposit_expiration_month,
    sum(cat.total_users) as total_category_booked_beneficiaries,
    mod(abs(sum(cat.cell_key)), 256) as cell_key_category
from {{ ref("mrt_native__outgoing_cohort") }} as oc
cross join unnest(oc.category_booked_users) as cat
inner join {{ ref("region_department") }} as rd on oc.user_department_code = rd.num_dep
group by
    date_trunc(date(oc.user_expiration_month), month),
    oc.user_is_in_qpv,
    oc.user_macro_density_label,
    oc.user_density_label,
    oc.user_region_name,
    rd.region_code,
    oc.user_department_name,
    oc.user_department_code,
    oc.user_epci,
    oc.user_epci_code,
    oc.user_city,
    oc.user_city_code,
    oc.user_municipality_label,
    oc.user_municipality_code,
    cat.offer_category_id
