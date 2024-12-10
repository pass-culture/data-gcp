CREATE OR REPLACE TABLE analytics.yearly_aggregated_venue_revenue ON cluster default
ENGINE = SummingMergeTree()
PARTITION BY year
ORDER BY (venue_id)
SETTINGS storage_policy = 'gcs_main'
AS
WITH
    -- Generate a sequence of years from 2020 to the current year
    year_spans AS (
        SELECT
            toStartOfYear(toDate(CONCAT(toString(2020), '-01-01')) + INTERVAL number YEAR) AS year
        FROM numbers(DATEDIFF(year, toDate('2020-01-01'), toStartOfYear(today())) + 1)
    )
SELECT
    s.year AS year,
    COALESCE(c.venue_id, i.venue_id) AS venue_id,
    SUM(COALESCE(c.revenue, 0)) AS collective_revenue,
    SUM(COALESCE(i.revenue, 0)) AS individual_revenue,
    SUM(COALESCE(c.expected_revenue, 0)) AS collective_expected_revenue,
    SUM(COALESCE(i.expected_revenue, 0)) AS individual_expected_revenue,
    SUM(COALESCE(c.revenue, 0) + COALESCE(i.revenue, 0)) AS total_revenue,
    SUM(COALESCE(c.expected_revenue, 0) + COALESCE(i.expected_revenue, 0)) AS total_expected_revenue
FROM
    year_spans s
LEFT JOIN analytics.yearly_aggregated_venue_collective_revenue c
    ON s.year = toStartOfYear(c.year)
LEFT JOIN analytics.yearly_aggregated_venue_individual_revenue i
    ON s.year = toStartOfYear(i.year)
WHERE venue_id is not null
GROUP BY
    1,2
ORDER BY
    1,2
