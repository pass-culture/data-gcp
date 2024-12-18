CREATE OR REPLACE TABLE analytics.yearly_aggregated_venue_collective_revenue ON cluster default
    ENGINE = SummingMergeTree()
    PARTITION BY year
    ORDER BY (venue_id)
    SETTINGS storage_policy='gcs_main'
AS
SELECT
    toStartOfYear(coalesce(toDate(used_date), toDate(creation_date))) AS year,
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
GROUP BY 1,2
