select
    user_region_name as region_name,
    user_department_name as department_name,
    user_department_code as department_code,
    user_epci_code as epci_code,
    user_epci as epci_name,
    user_city_code as city_code,
    user_city as city_name,
    user_is_in_qpv as is_in_qpv,
    user_density_label as micro_density_label,
    user_macro_density_label as macro_density_label,
    date_trunc(date(last_deposit_expiration_date), month) as partition_month,
    coalesce(count(distinct user_id), 0) as total_expired_beneficiaries,
    coalesce(sum(total_deposit_amount), 0) as total_deposit_amount_at_expiration,
    coalesce(
        safe_divide(sum(total_deposit_amount), count(distinct user_id)), 0
    ) as average_deposit_amount_at_expiration,
    coalesce(
        sum(total_actual_amount_spent), 0
    ) as total_actual_amount_spent_at_expiration,
    coalesce(
        safe_divide(sum(total_actual_amount_spent), count(distinct user_id)), 0
    ) as average_actual_amount_spent_at_expiration
from {{ ref("int_global__user_beneficiary") }}
where
    (user_is_active or user_suspension_reason = 'upon user request')
    and current_deposit_type != 'GRANT_FREE'
    and current_deposit_type in ('GRANT_18', 'GRANT_17_18')
group by
    partition_month,
    region_name,
    department_name,
    department_code,
    epci_code,
    epci_name,
    city_code,
    city_name,
    is_in_qpv,
    micro_density_label,
    macro_density_label
