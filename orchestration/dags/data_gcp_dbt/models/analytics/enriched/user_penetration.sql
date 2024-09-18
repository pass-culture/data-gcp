-- temporary fix for CI, query unable to be run as view
{{ config(tags='failing_ci') }}

with population_dpt as (
    select
        DATE(pop.current_date) active_month,
        pop.decimal_age,
        DATE(pop.born_date) as born_date,
        pop.department_code,
        pop.department_name,
        dep.region_name,
        SUM(population) as population
    from {{ source('seed','population_age_and_department_france_details') }} pop
        left join {{ source('seed','region_department') }} dep on dep.num_dep = pop.department_code
    where pop.current_year in (2020, 2021, 2022, 2023, 2024) and CAST(age as int) between 15 and 25
    group by 1, 2, 3, 4, 5, 6
),

user_booking as (
    select
        aa.active_month,
        aa.user_department_code as department_code,
        DATE(DATE_TRUNC(ud.user_birth_date, month)) as born_date,
        COUNT(distinct ud.user_id) as total_users
    from {{ ref('aggregated_monthly_user_used_booking_activity') }} aa
        inner join {{ ref('mrt_global__user') }} ud on ud.user_id = aa.user_id
    group by 1, 2, 3
)

select
    pop.active_month,
    pop.born_date,
    pop.decimal_age,
    pop.department_code,
    COALESCE(ub.total_users, 0) as total_users,
    case
        when pop.decimal_age >= 15 and pop.decimal_age < 18 then "15_17"
        when pop.decimal_age >= 18 and pop.decimal_age < 20 then "18_19"
        else "20_25"
    end as age_range,
    COALESCE(population, 0) as population
from population_dpt pop
    left join user_booking ub on
        pop.active_month = ub.active_month
        and pop.born_date = ub.born_date
        and pop.department_code = ub.department_code
