CREATE OR REPLACE TABLE analytics.monthly_aggregated_venue_collective_revenue ON cluster default
    ENGINE = SummingMergeTree()
    PARTITION BY expected_month
    ORDER BY (venue_id)
    SETTINGS storage_policy='gcs_main'
AS
SELECT
    date_trunc('MONTH', COALESCE(toDate (scheduled_date),toDate (used_dad), max(current_date(),toDate (creation_date)))) AS expected_month,
    cast(venue_id as String) as venue_id,
    sum(
        case
            when collective_booking_status = 'USED'
            or collective_booking_status = 'REIMBURSED' then booking_amount
            else 0
        end
    ) AS revenue,
    sum(booking_amount) AS expected_revenue
FROM
    intermediate.collective_booking
WHERE
    venue_id IS NOT NULL
GROUP BY
    1,2
