CREATE OR REPLACE TABLE analytics.yearly_aggregated_venue_individual_revenue ON cluster default
    ENGINE = SummingMergeTree()
    PARTITION BY creation_year
    ORDER BY (venue_id)
    SETTINGS storage_policy='gcs_main'
AS
SELECT
    date_trunc('YEAR', toDate (creation_date)) AS creation_year,
    cast(venue_id as String) as venue_id,
    sum(
        case
            when booking_status = 'USED'
            or booking_status = 'REIMBURSED' then booking_amount*booking_quantity
            else 0
        end
    ) AS revenue,
    sum(booking_amount*booking_quantity) AS expected_revenue
FROM
    intermediate.booking
WHERE
    venue_id IS NOT NULL
GROUP BY 1,2
