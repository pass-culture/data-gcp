{% set secret_threshold_beneficiary = 5 %}

with
    expired_users as (
        select
            ub.user_id,
            ub.user_is_in_qpv as is_in_qpv,
            ub.user_macro_density_label as macro_density_label,
            ub.user_density_label as micro_density_label,
            ub.user_department_code as department_code,
            rd.dep_name as department_name,
            rd.region_name,
            rd.region_code,
            date_trunc(
                ub.last_deposit_expiration_date, month
            ) as deposit_expiration_month
        from {{ ref("int_global__user_beneficiary") }} as ub
        left join
            {{ ref("region_department") }} as rd on ub.user_department_code = rd.num_dep
        where
            ub.total_deposit_amount >= 300
            and ub.last_deposit_expiration_date is not null
            and (ub.user_is_active or ub.user_suspension_reason = "upon user request")
            and ub.current_deposit_type != "GRANT_FREE"
    ),

    user_booked_categories as (
        select distinct user_id, offer_category_id
        from {{ ref("int_global__booking") }}
        where booking_is_used
    ),

    final_data as (
        select
            u.deposit_expiration_month,
            u.is_in_qpv,
            u.macro_density_label,
            u.micro_density_label,
            u.region_name,
            u.region_code,
            u.department_name,
            u.department_code,
            cat.offer_category_id,
            count(
                distinct case
                    when cat.offer_category_id is not null then cat.user_id
                end
            ) as total_category_booked_beneficiaries
        from expired_users as u
        left join user_booked_categories as cat on u.user_id = cat.user_id
        where cat.offer_category_id is not null
        group by
            u.deposit_expiration_month,
            u.is_in_qpv,
            u.macro_density_label,
            u.micro_density_label,
            u.region_name,
            u.region_code,
            u.department_name,
            u.department_code,
            cat.offer_category_id
    )

select
    deposit_expiration_month,
    case
        when total_category_booked_beneficiaries <= {{ secret_threshold_beneficiary }}
        then true
        else false
    end as is_statistc_secret,
    region_name,
    region_code,
    department_name,
    department_code,
    is_in_qpv,
    macro_density_label,
    micro_density_label,
    offer_category_id,
    total_category_booked_beneficiaries
from final_data
