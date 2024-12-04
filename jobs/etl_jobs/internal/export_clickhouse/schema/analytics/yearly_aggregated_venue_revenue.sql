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
            toDate(CONCAT(toString(number + 2020), '-01-01')) AS year
        FROM numbers((YEAR(today()) - 2020) + 1)
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
    ON s.year = c.year
LEFT JOIN analytics.yearly_aggregated_venue_individual_revenue i
    ON s.year = i.year
GROUP BY
    s.year,
    COALESCE(c.venue_id, i.venue_id)
ORDER BY
    venue_id,
    s.year
