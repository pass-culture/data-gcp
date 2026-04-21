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

    total_users_base as (
        select
            eud.user_id,
            ldm.partition_month,
            rd.region_name,
            rd.region_code,
            rd.dep_name as department_name,
            rd.num_dep as department_code,
            eud.user_epci as epci_name,
            eud.user_epci_code as epci_code,
            eud.user_city as city_name,
            eud.user_city_code as city_code,
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
        from last_day_of_month as ldm
        inner join
            {{ ref("int_global__user_beneficiary") }} as eud
            on date(eud.first_deposit_creation_date) <= date(ldm.last_active_date)
            and (eud.user_is_active or eud.user_suspension_reason = "upon user request")
            and eud.current_deposit_type != "GRANT_FREE"
        left join
            {{ ref("region_department") }} as rd
            on eud.user_department_code = rd.num_dep
    ),

    active_users_base as (
        select uua.user_id, ldm.partition_month
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
    )

select
    tub.partition_month,
    tub.region_name,
    tub.region_code,
    tub.department_name,
    tub.department_code,
    tub.epci_name,
    tub.epci_code,
    tub.city_name,
    tub.city_code,
    tub.age_at_calculation,
    tub.is_in_qpv,
    tub.macro_density_label,
    tub.micro_density_label,
    count(distinct aub.user_id) as total_actual_beneficiaries,
    count(distinct tub.user_id) as total_beneficiaries,
    mod(
        abs(sum(distinct {{ record_key("tub.user_id") }})), 256
    ) as cell_key_beneficiaries,
    coalesce(
        mod(abs(sum(distinct {{ record_key("aub.user_id") }})), 256), 0
    ) as cell_key_actual_beneficiaries
from total_users_base as tub
left join
    active_users_base as aub
    on tub.user_id = aub.user_id
    and tub.partition_month = aub.partition_month
group by
    tub.partition_month,
    tub.region_name,
    tub.region_code,
    tub.department_name,
    tub.department_code,
    tub.epci_name,
    tub.epci_code,
    tub.city_name,
    tub.city_code,
    tub.age_at_calculation,
    tub.is_in_qpv,
    tub.macro_density_label,
    tub.micro_density_label
