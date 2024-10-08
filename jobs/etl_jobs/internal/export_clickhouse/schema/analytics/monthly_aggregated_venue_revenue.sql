CREATE OR REPLACE TABLE analytics.monthy_aggregated_venue_revenue ON cluster default
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
            when booking_status = 'USED'
            or booking_status = 'REIMBURSED' then booking_amount
            else 0
        end
    ) AS revenue,
    sum(booking_amount) AS expected_revenue
FROM
    intermediate.booking
GROUP BY 1,2
