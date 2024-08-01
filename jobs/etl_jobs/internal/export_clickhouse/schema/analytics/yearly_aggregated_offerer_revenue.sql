CREATE OR REPLACE TABLE analytics.yearly_aggregated_offerer_revenue ON cluster default
    ENGINE = MergeTree
    PARTITION BY creation_year
    ORDER BY (offerer_id)
    SETTINGS storage_policy='gcs_main'
AS
SELECT
    date_trunc('YEAR', toDate (creation_date)) AS creation_year,
    cast(offerer_id as String) as offerer_id,
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