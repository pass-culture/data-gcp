CREATE OR REPLACE TABLE analytics.monthy_aggregated_venue_collective_revenue ON cluster default
    ENGINE = SummingMergeTree()
    PARTITION BY creation_month
    ORDER BY (venue_id)
    SETTINGS storage_policy='gcs_main'
AS
SELECT
    date_trunc('MONTH', toDate (creation_date)) AS creation_month,
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
