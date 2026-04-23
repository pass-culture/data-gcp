with
    final_data as (
        select
            oc.user_region_name as region_name,
            rd.region_code,
            oc.user_department_name as department_name,
            oc.user_department_code as department_code,
            oc.user_epci as epci_name,
            oc.user_epci_code as epci_code,
            oc.user_city as city_name,
            oc.user_city_code as city_code,
            oc.user_is_in_qpv as is_in_qpv,
            oc.user_macro_density_label as macro_density_label,
            oc.user_density_label as micro_density_label,
            date_trunc(
                date(oc.user_expiration_month), month
            ) as deposit_expiration_month,
            coalesce(
                sum(oc.total_3_category_booked_users), 0
            ) as total_3plus_category_booked_beneficiaries,
            coalesce(sum(oc.total_users), 0) as total_expired_credit_beneficiaries,
            coalesce(mod(abs(sum(oc.cell_key_3_category)), 256), 0) as cell_key_3plus,
            mod(abs(sum(oc.cell_key_users)), 256) as cell_key_expired
        from {{ ref("mrt_native__outgoing_cohort") }} as oc
        left join
            {{ ref("region_department") }} as rd on oc.user_department_code = rd.num_dep
        group by
            date_trunc(date(oc.user_expiration_month), month),
            oc.user_region_name,
            rd.region_code,
            oc.user_department_name,
            oc.user_department_code,
            oc.user_epci,
            oc.user_epci_code,
            oc.user_city,
            oc.user_city_code,
            oc.user_is_in_qpv,
            oc.user_macro_density_label,
            oc.user_density_label
    )

select
    deposit_expiration_month,
    region_name,
    region_code,
    department_name,
    department_code,
    epci_name,
    epci_code,
    city_name,
    city_code,
    is_in_qpv,
    macro_density_label,
    micro_density_label,
    total_3plus_category_booked_beneficiaries,
    total_expired_credit_beneficiaries,
    cell_key_3plus,
    cell_key_expired
from final_data
