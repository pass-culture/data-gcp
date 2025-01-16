CREATE OR REPLACE TABLE analytics.yearly_aggregated_venue_revenue ON cluster default
ENGINE = SummingMergeTree()
PARTITION BY year
ORDER BY (venue_id)
SETTINGS storage_policy = 'gcs_main'
AS
WITH
    -- Generate a sequence of years from 2018 to the current year
    year_spans AS (
        SELECT
            toStartOfYear(toDate(CONCAT('2018-01-01')) + INTERVAL number YEAR) AS year
        FROM numbers(0,DATEDIFF(year, toDate('2018-01-01'), toStartOfYear(today())) + 1)
    ),
revenue_union as (
    SELECT
        venue_id,
        toStartOfYear(year) AS year,
        revenue AS collective_revenue,
        0 AS individual_revenue,
        expected_revenue AS collective_expected_revenue,
        0 AS individual_expected_revenue
    FROM
        analytics.yearly_aggregated_venue_collective_revenue
    UNION ALL
    SELECT
        venue_id,
        toStartOfYear(year) AS year,
        0 AS collective_revenue,
        revenue AS individual_revenue,
        0 AS collective_expected_revenue,
        expected_revenue AS individual_expected_revenue
    FROM
        analytics.yearly_aggregated_venue_individual_revenue
)
SELECT
    s.year AS year,
    ru.venue_id AS venue_id,
    SUM(ru.collective_revenue) AS collective_revenue,
    SUM(ru.individual_revenue) AS individual_revenue,
    SUM(ru.collective_expected_revenue) AS collective_expected_revenue,
    SUM(ru.individual_expected_revenue) AS individual_expected_revenue,
    SUM(ru.collective_revenue + ru.individual_revenue) AS total_revenue,
    SUM(ru.collective_expected_revenue + ru.individual_expected_revenue) AS total_expected_revenue
FROM
    year_spans s
LEFT JOIN revenue_union ru
    ON s.year = ru.year
GROUP BY
    1,2
ORDER BY
    1,2
