with cohorted_population as (
    select
        DATE(active_month) as active_month,
        CAST(decimal_age as STRING) as decimal_age,
        department_code,
        SUM(total_users) total_users,
        SUM(population) population
    from {{ ref('user_penetration') }}
    where decimal_age in (15, 15.5, 16, 16.5, 17, 17.5, 18, 18.5, 19)
    group by 1, 2, 3
)

select
    active_month,
    decimal_age,
    department_code,
    total_users,
    SUM(total_users) over (
        partition by decimal_age, department_code
        order by active_month rows between 11 preceding and current row
    ) as total_users_last_12_months,
    population,
    SUM(population) over (
        partition by decimal_age, department_code
        order by active_month rows between 11 preceding and current row
    ) as population_last_12_months

from cohorted_population
