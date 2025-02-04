-- TODO: deprecated
-- Temporary fix for CI, query unable to be run as view
{{ config(tags="failing_ci") }}

with
    -- Population data aggregated by department
    population_dpt as (
        select
            pop.decimal_age as decimal_age,  -- Keep legacy before removal
            pop.department_code,
            pop.department_name,
            dep.region_name,
            date(pop.current_date) as active_month,
            date(pop.born_date) as born_date,
            sum(pop.population) as population
        from {{ source("seed", "population_age_and_department_france_details") }} as pop
        left join
            {{ source("seed", "region_department") }} as dep
            on pop.department_code = dep.num_dep
        where
            pop.current_year in (2020, 2021, 2022, 2023, 2024, 2025)
            and cast(pop.age as int) between 15 and 25
        group by
            date(pop.current_date),
            date(pop.born_date),
            pop.decimal_age,
            pop.department_code,
            pop.department_name,
            dep.region_name
    ),

    -- User booking activity aggregated by department
    user_booking as (
        select
            aa.active_month,
            aa.user_department_code as department_code,
            date(date_trunc(ud.user_birth_date, month)) as born_date,
            count(distinct ud.user_id) as total_users
        from {{ ref("aggregated_monthly_user_used_booking_activity") }} as aa
        inner join {{ ref("mrt_global__user") }} as ud on aa.user_id = ud.user_id
        group by
            aa.active_month,
            aa.user_department_code,
            date(date_trunc(ud.user_birth_date, month))
    )

select
    pop.active_month,
    pop.born_date,
    pop.decimal_age,
    pop.department_code,
    coalesce(pop.population, 0) as population,
    coalesce(ub.total_users, 0) as total_users,
    case
        when pop.decimal_age >= 15 and pop.decimal_age < 18
        then '15_17'
        when pop.decimal_age >= 18 and pop.decimal_age < 20
        then '18_19'
        else '20_25'
    end as age_range
from population_dpt as pop
left join
    user_booking as ub
    on pop.active_month = ub.active_month
    and pop.born_date = ub.born_date
    and pop.department_code = ub.department_code
