{{
    config(
        materialized='table',
        schema='analytics_' ~ target.name
    )
}}

-- Generate a daily time spine from 2020-01-01 to 3 years in the future
-- This covers historical data and allows for future-dated bookings

WITH date_spine AS (
    SELECT
        date_day
    FROM
        UNNEST(
            GENERATE_DATE_ARRAY(
                '2020-01-01',
                DATE_ADD(CURRENT_DATE(), INTERVAL 3 YEAR),
                INTERVAL 1 DAY
            )
        ) AS date_day
)

SELECT
    date_day AS date_day
FROM date_spine
