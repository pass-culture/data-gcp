CREATE OR REPLACE TABLE analytics.monthly_aggregated_venue_revenue ON cluster default
ENGINE = SummingMergeTree()
PARTITION BY creation_month
ORDER BY (venue_id)
SETTINGS storage_policy = 'gcs_main'
AS
WITH
    -- Generate a sequence of months from January 2020 to the current month
    month_spans AS (
        SELECT
            toStartOfMonth(toDate(CONCAT(toString(2020), '-01-01')) + INTERVAL number MONTH) AS creation_month
        FROM numbers(DATEDIFF(MONTH, toDate('2020-01-01'), toStartOfMonth(today())) + 1)
    )
SELECT
    s.creation_month AS creation_month,
    COALESCE(c.venue_id, i.venue_id) AS venue_id,
    SUM(COALESCE(c.revenue, 0)) AS collective_revenue,
    SUM(COALESCE(i.revenue, 0)) AS individual_revenue,
    SUM(COALESCE(c.expected_revenue, 0)) AS collective_expected_revenue,
    SUM(COALESCE(i.expected_revenue, 0)) AS individual_expected_revenue,
    SUM(COALESCE(c.revenue, 0) + COALESCE(i.revenue, 0)) AS total_revenue,
    SUM(COALESCE(c.expected_revenue, 0) + COALESCE(i.expected_revenue, 0)) AS total_expected_revenue
FROM
    month_spans s
LEFT JOIN analytics.yearly_aggregated_venue_collective_revenue c
    ON s.creation_month = toStartOfMonth(c.creation_year)
LEFT JOIN analytics.yearly_aggregated_venue_individual_revenue i
    ON s.creation_month = toStartOfMonth(i.creation_year)
GROUP BY
    s.creation_month,
    COALESCE(c.venue_id, i.venue_id)
ORDER BY
    s.creation_month,
    venue_id;
