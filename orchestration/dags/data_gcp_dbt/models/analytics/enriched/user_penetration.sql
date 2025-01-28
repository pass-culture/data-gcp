-- Temporary fix for CI, query unable to be run as view
{{ config(tags="failing_ci") }}

WITH
    -- Population data aggregated by department
    population_dpt AS (
        SELECT
            pop.decimal_age,
            pop.department_code,
            pop.department_name,
            dep.region_name,
            DATE(pop.current_date) AS active_month,
            DATE(pop.born_date) AS born_date,
            SUM(pop.population) AS population
        FROM {{ source("seed", "population_age_and_department_france_details") }} AS pop
        LEFT JOIN {{ source("seed", "region_department") }} AS dep
            ON pop.department_code = dep.num_dep
        WHERE
            pop.current_year IN (2020, 2021, 2022, 2023, 2024, 2025)
            AND CAST(pop.age AS INT) BETWEEN 15 AND 25
        GROUP BY
            DATE(pop.current_date),
            DATE(pop.born_date),
            pop.decimal_age,
            pop.department_code,
            pop.department_name,
            dep.region_name
    ),

    -- User booking activity aggregated by department
    user_booking AS (
        SELECT
            aa.active_month,
            aa.user_department_code AS department_code,
            DATE(DATE_TRUNC('month', ud.user_birth_date)) AS born_date,
            COUNT(DISTINCT ud.user_id) AS total_users
        FROM {{ ref("aggregated_monthly_user_used_booking_activity") }} AS aa
        INNER JOIN {{ ref("mrt_global__user") }} AS ud
            ON aa.user_id = ud.user_id
        GROUP BY
            aa.active_month,
            aa.user_department_code,
            DATE(DATE_TRUNC('month', ud.user_birth_date))
    )

SELECT
    pop.active_month,
    pop.born_date,
    pop.decimal_age,
    pop.department_code,
    COALESCE(pop.population, 0) AS population,
    COALESCE(ub.total_users, 0) AS total_users,
    CASE
        WHEN pop.decimal_age >= 15 AND pop.decimal_age < 18 THEN '15_17'
        WHEN pop.decimal_age >= 18 AND pop.decimal_age < 20 THEN '18_19'
        ELSE '20_25'
    END AS age_range
FROM population_dpt AS pop
LEFT JOIN user_booking AS ub
    ON pop.active_month = ub.active_month
    AND pop.born_date = ub.born_date
    AND pop.department_code = ub.department_code
;
