CREATE OR REPLACE TABLE analytics.monthly_aggregated_venue_individual_revenue ON cluster default
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
            WHEN booking_status IN ('USED', 'REIMBURSED') THEN booking_amount * booking_quantity
            ELSE 0
        END
    ) AS revenue,
    sum(booking_amount * booking_quantity) AS expected_revenue
FROM
    intermediate.booking
WHERE
    venue_id IS NOT NULL
GROUP BY
    1,2
