CREATE OR REPLACE TABLE analytics.yearly_aggregated_venue_revenue ON cluster default
    ENGINE = SummingMergeTree()
    PARTITION BY expected_year
    ORDER BY (venue_id)
    SETTINGS storage_policy='gcs_main'
AS
SELECT
    i.expected_year,
    cast(i.venue_id as String) as venue_id,
    sum(coalesce(i.revenue,0)) as individual_revenue,
    sum(coalesce(c.revenue,0)) as collective_revenue,
    sum(coalesce(i.revenue,0) + coalesce(c.revenue,0)) as total_revenue,
    sum(coalesce(i.expected_revenue,0)) as individual_expected_revenue,
    sum(coalesce(c.expected_revenue,0)) as collective_expected_revenue,
    sum(coalesce(i.expected_revenue,0) + coalesce(c.expected_revenue,0)) as total_expected_revenue

FROM
    analytics.yearly_aggregated_venue_individual_revenue i
LEFT JOIN
    analytics.yearly_aggregated_venue_collective_revenue c
ON i.expected_year = c.expected_year AND i.venue_id = c.venue_id
GROUP BY
    1,2
