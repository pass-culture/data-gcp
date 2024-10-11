CREATE OR REPLACE TABLE analytics.monthy_aggregated_venue_revenue ON cluster default
    ENGINE = SummingMergeTree()
    PARTITION BY creation_month
    ORDER BY (venue_id)
    SETTINGS storage_policy='gcs_main'
AS
SELECT
    creation_month,
    cast(i.venue_id as String) as venue_id,
    coalesce(i.revenue,0) as individual_revenue,
    coalesce(c.revenue,0) as collective_revenue,
    sum(coalesce(i.revenue,0) + coalesce(c.revenue,0)) as total_revenue,
    coalesce(i.expected_revenue,0) as individual_expected_revenue,
    coalesce(c.expected_revenue,0) as collective_expected_revenue,
    sum(coalesce(i.expected_revenue,0) + coalesce(c.expected_revenue,0)) as total_expected_revenue

FROM
    analytics.monthy_aggregated_venue_individual_revenue i
LEFT JOIN
    analytics.monthy_aggregated_venue_collective_revenue c
ON i.creation_month = c.creation_month AND i.venue_id = c.venue_id
GROUP BY 
    1,2 

