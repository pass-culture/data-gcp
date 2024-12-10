CREATE OR REPLACE TABLE analytics.monthly_aggregated_venue_collective_revenue ON cluster default
ENGINE = SummingMergeTree()
PARTITION BY month
ORDER BY (venue_id)
SETTINGS storage_policy='gcs_main'
AS
SELECT
    toStartOfMonth(coalesce(toDate(used_date), toDate(creation_date))) AS month,
    cast(venue_id AS String) AS venue_id,
    sum(
        CASE
            WHEN collective_booking_status IN ('USED', 'REIMBURSED') THEN booking_amount
            ELSE 0
        END
    ) AS revenue,
    sum(booking_amount) AS expected_revenue
FROM
    intermediate.collective_booking
WHERE
    venue_id IS NOT NULL
GROUP BY
    month, venue_id;
