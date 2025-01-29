with
    user_deposit as (
        select
            dud.user_department_code as department_code,
            date(date_trunc(dud.user_snapshot_date, month)) as snapshot_month,
            date(date_trunc(dud.user_birth_date, month)) as birth_month,
            count(distinct dud.user_id) as total_users
        from {{ ref("mrt_native__daily_user_deposit") }} as dud
        where user_snapshot_date > date_sub(current_date(), interval 48 month)  -- 4 years
        group by
            date(date_trunc(dud.user_snapshot_date, month)),
            date(date_trunc(dud.user_birth_date, month)),
            dud.user_department_code
    ),

    beneficiary_coverage as (
        select
            pop.snapshot_month,
            pop.population_birth_month,
            cast(pop.population_decimal_age as string) as population_decimal_age,
            pop.department_code,
            pop.region_name,
            pop.academy_name,
            coalesce(pop.population, 0) as total_population,
            coalesce(ub.total_users, 0) as total_users,
            case
                when
                    pop.population_decimal_age >= 15 and pop.population_decimal_age < 18
                then '15_17'
                when
                    pop.population_decimal_age >= 18 and pop.population_decimal_age < 20
                then '18_19'
                else '20_25'
            end as population_age_bracket,
            pop.population_decimal_age in (
                15, 15.5, 16, 16.5, 17, 17.5, 18, 18.5, 19
            ) as population_age_decimal_set
        from {{ ref("int_seed__monthly_france_population_snapshot") }} as pop
        left join
            user_deposit as ub
            on pop.snapshot_month = ub.snapshot_month
            and pop.population_birth_month = ub.birth_month
            and pop.department_code = ub.department_code
    )

select
    snapshot_month,
    population_birth_month,
    population_decimal_age,
    population_age_decimal_set,
    population_age_bracket,
    department_code,
    region_name,
    academy_name,
    total_users,
    total_population,
    sum(total_users) over (
        partition by population_decimal_age, department_code
        order by snapshot_month
        rows between 11 preceding and current row
    ) as total_users_last_12_months,
    sum(total_population) over (
        partition by population_decimal_age, department_code
        order by snapshot_month
        rows between 11 preceding and current row
    ) as total_population_last_12_months

from beneficiary_coverage
