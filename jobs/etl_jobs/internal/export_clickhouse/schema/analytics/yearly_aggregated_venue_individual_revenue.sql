CREATE OR REPLACE TABLE analytics.yearly_aggregated_venue_individual_revenue ON cluster default
    ENGINE = SummingMergeTree()
    PARTITION BY expected_year
    ORDER BY (venue_id)
    SETTINGS storage_policy='gcs_main'
AS
SELECT
    date_trunc('YEAR', COALESCE(toDate (scheduled_date),toDate (used_dad), max(current_date(),toDate (creation_date)))) AS expected_year,
    cast(venue_id as String) as venue_id,
    sum(
        case
            when booking_status = 'USED'
            or booking_status = 'REIMBURSED' then booking_amount
            else 0
        end
    ) AS revenue,
    sum(booking_amount) AS expected_revenue
FROM
    intermediate.booking
WHERE
    venue_id IS NOT NULL
GROUP BY 1,2
