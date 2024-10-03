with
    cohorted_population as (
        select
            date(active_month) as active_month,
            cast(decimal_age as string) as decimal_age,
            department_code,
            sum(total_users) total_users,
            sum(population) population
        from {{ ref("user_penetration") }}
        where decimal_age in (15, 15.5, 16, 16.5, 17, 17.5, 18, 18.5, 19)
        group by 1, 2, 3
    )

select
    active_month,
    decimal_age,
    department_code,
    total_users,
    sum(total_users) over (
        partition by decimal_age, department_code
        order by active_month
        rows between 11 preceding and current row
    ) as total_users_last_12_months,
    population,
    sum(population) over (
        partition by decimal_age, department_code
        order by active_month
        rows between 11 preceding and current row
    ) as population_last_12_months

from cohorted_population
