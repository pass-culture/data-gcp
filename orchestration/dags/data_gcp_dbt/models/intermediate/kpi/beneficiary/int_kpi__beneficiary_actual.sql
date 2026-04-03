{% set secret_threshold_beneficiary = 5 %}

with

    last_day_of_month as (
        select
            date_trunc(deposit_active_date, month) as partition_month,
            max(deposit_active_date) as last_active_date
        from {{ ref("mrt_native__daily_user_deposit") }}
        where deposit_active_date > date("2021-01-01")
        group by date_trunc(deposit_active_date, month)
    ),

    user_amount_spent_per_day as (
        select
            uua.deposit_active_date,
            uua.user_id,
            uua.deposit_amount,
            case
                when uua.deposit_type = "GRANT_17_18" and uua.user_age <= 17
                then "GRANT_15_17"
                when uua.deposit_type = "GRANT_17_18" and uua.user_age >= 18
                then "GRANT_18"
                else uua.deposit_type
            end as deposit_type,
            coalesce(sum(ebd.booking_intermediary_amount), 0) as amount_spent
        from {{ ref("mrt_native__daily_user_deposit") }} as uua
        left join
            {{ ref("int_global__booking") }} as ebd
            on uua.deposit_id = ebd.deposit_id
            and uua.deposit_active_date = date(ebd.booking_used_date)
            and ebd.booking_is_used
        where uua.deposit_active_date > date("2021-01-01")
        group by uua.deposit_active_date, uua.user_id, deposit_type, uua.deposit_amount
    ),

    user_cumulative_amount_spent as (
        select
            deposit_active_date,
            user_id,
            deposit_type,
            deposit_amount as initial_deposit_amount,
            sum(amount_spent) over (
                partition by user_id, deposit_type order by deposit_active_date asc
            ) as cumulative_amount_spent
        from user_amount_spent_per_day
    ),

    active_users_base as (
        select
            uua.user_id,
            ldm.partition_month,
            rd.region_name,
            rd.region_code,
            rd.dep_name as department_name,
            rd.num_dep as department_code,
            eud.user_is_in_qpv as is_in_qpv,
            eud.user_macro_density_label as macro_density_label,
            eud.user_density_label as micro_density_label,
            date_diff(ldm.partition_month, eud.user_birth_date, year) - if(
                extract(month from eud.user_birth_date)
                > extract(month from ldm.partition_month)
                or (
                    extract(month from eud.user_birth_date)
                    = extract(month from ldm.partition_month)
                    and extract(day from eud.user_birth_date)
                    > extract(day from ldm.partition_month)
                ),
                1,
                0
            ) as age_at_calculation
        from user_cumulative_amount_spent as uua
        inner join
            last_day_of_month as ldm on uua.deposit_active_date = ldm.last_active_date
        inner join
            {{ ref("int_global__user_beneficiary") }} as eud
            on uua.user_id = eud.user_id
            and (eud.user_is_active or eud.user_suspension_reason = "upon user request")
            and eud.current_deposit_type != "GRANT_FREE"
        left join
            {{ ref("region_department") }} as rd
            on eud.user_department_code = rd.num_dep
        where uua.cumulative_amount_spent < uua.initial_deposit_amount
    ),

    final_data as (
        select
            partition_month,
            region_name,
            region_code,
            department_name,
            department_code,
            age_at_calculation,
            is_in_qpv,
            macro_density_label,
            micro_density_label,
            count(distinct user_id) as total_actual_beneficiaries
        from active_users_base
        where age_at_calculation between 15 and 22
        group by
            partition_month,
            region_name,
            region_code,
            department_name,
            department_code,
            age_at_calculation,
            is_in_qpv,
            macro_density_label,
            micro_density_label
    )

select
    partition_month,
    case
        when total_actual_beneficiaries <= {{ secret_threshold_beneficiary }}
        then "secret_statistique"
        else cast(region_name as string)
    end as region_name,
    case
        when total_actual_beneficiaries <= {{ secret_threshold_beneficiary }}
        then "secret_statistique"
        else cast(region_code as string)
    end as region_code,
    case
        when total_actual_beneficiaries <= {{ secret_threshold_beneficiary }}
        then "secret_statistique"
        else cast(department_name as string)
    end as department_name,
    case
        when total_actual_beneficiaries <= {{ secret_threshold_beneficiary }}
        then "secret_statistique"
        else cast(department_code as string)
    end as department_code,
    age_at_calculation,
    is_in_qpv,
    macro_density_label,
    micro_density_label,
    total_actual_beneficiaries
from final_data
