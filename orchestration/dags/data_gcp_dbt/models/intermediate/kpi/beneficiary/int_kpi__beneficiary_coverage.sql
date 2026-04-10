{% set secret_threshold_beneficiary = 5 %}

with
    user_deposit as (
        select
            dud.user_department_code as department_code,
            date(date_trunc(dud.deposit_active_date, month)) as deposit_active_month,
            date_diff(dud.deposit_active_date, dud.user_birth_date, month)
            / 12.0 as user_decimal_age,
            count(distinct dud.user_id) as total_beneficiaries
        from {{ ref("mrt_native__daily_user_deposit") }} as dud
        where dud.deposit_active_date > date_sub(current_date(), interval 48 month)  -- 4 years
        group by
            date(date_trunc(dud.deposit_active_date, month)),
            user_decimal_age,
            dud.user_department_code
    ),

    beneficiary_coverage as (
        select
            ub.deposit_active_month,
            cast(ub.user_decimal_age as string) as milestone_age,
            ub.department_code,
            rd.dep_name as department_name,
            rd.region_name,
            rd.region_code,
            ub.total_beneficiaries
        from user_deposit as ub
        left join
            {{ ref("region_department") }} as rd on ub.department_code = rd.num_dep
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
            ) as total_beneficiaries_last_12_months
        from beneficiary_coverage
        where milestone_age in ("16", "17", "18", "19", "20")
    )

select
    partition_month,
    case
        when total_beneficiaries_last_12_months <= {{ secret_threshold_beneficiary }}
        then true
        else false
    end as is_statistic_secret,
    region_name,
    region_code,
    department_name,
    department_code,
    milestone_age,
    total_beneficiaries_last_12_months
from final_data
