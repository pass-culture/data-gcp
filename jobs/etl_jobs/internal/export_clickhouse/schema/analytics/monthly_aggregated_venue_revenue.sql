CREATE OR REPLACE TABLE analytics.monthly_aggregated_venue_revenue ON cluster default
ENGINE = SummingMergeTree()
PARTITION BY month
ORDER BY (venue_id)
SETTINGS storage_policy = 'gcs_main'
AS
WITH
    -- Generate a sequence of months from January 2020 to the current month
    month_spans AS (
        SELECT
            toStartOfMonth(toDate(CONCAT(toString(2020), '-01-01')) + INTERVAL number MONTH) AS month
        FROM numbers(DATEDIFF(month, toDate('2020-01-01'), toStartOfMonth(today())) + 1)
    )
SELECT
    s.month AS month,
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
    ON s.month = c.month
LEFT JOIN analytics.yearly_aggregated_venue_individual_revenue i
    ON s.month = i.month
WHERE COALESCE(c.venue_id, i.venue_id) is not null
GROUP BY
    s.month,
    COALESCE(c.venue_id, i.venue_id)
ORDER BY
    s.month,
    venue_id;
