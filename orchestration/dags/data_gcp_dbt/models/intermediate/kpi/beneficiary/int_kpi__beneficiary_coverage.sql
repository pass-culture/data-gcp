with
    user_deposit as (
        select
            dud.user_id,
            dud.user_department_code as department_code,
            date(date_trunc(dud.deposit_active_date, month)) as deposit_active_month,
            date_diff(dud.deposit_active_date, dud.user_birth_date, month)
            / 12.0 as user_decimal_age
        from {{ ref("mrt_native__daily_user_deposit") }} as dud
        where dud.deposit_active_date > date_sub(current_date(), interval 48 month)
    ),

    beneficiary_coverage as (
        select
            ub.deposit_active_month,
            cast(ub.user_decimal_age as string) as milestone_age,
            ub.department_code,
            rd.dep_name as department_name,
            rd.region_name,
            rd.region_code,
            count(distinct ub.user_id) as total_beneficiaries,
            mod(
                abs(sum(distinct {{ record_key("ub.user_id") }})), 256
            ) as cell_key_beneficiaries
        from user_deposit as ub
        left join
            {{ ref("region_department") }} as rd on ub.department_code = rd.num_dep
        group by
            ub.deposit_active_month,
            ub.user_decimal_age,
            ub.department_code,
            rd.dep_name,
            rd.region_name,
            rd.region_code
    ),

    final_data as (
        select
            deposit_active_month as partition_month,
            region_name,
            region_code,
            department_name,
            department_code,
            cast(milestone_age as integer) as milestone_age,
            sum(total_beneficiaries) over (
                partition by milestone_age, department_code
                order by deposit_active_month
                rows between 11 preceding and current row
            ) as total_beneficiaries_last_12_months,
            sum(cell_key_beneficiaries) over (
                partition by milestone_age, department_code
                order by deposit_active_month
                rows between 11 preceding and current row
            ) as cell_key_rolling
        from beneficiary_coverage
        where milestone_age in ("16", "17", "18", "19", "20")
    )

select
    partition_month,
    region_name,
    region_code,
    department_name,
    department_code,
    milestone_age,
    total_beneficiaries_last_12_months,
    mod(abs(cell_key_rolling), 256) as cell_key_beneficiaries
from final_data
