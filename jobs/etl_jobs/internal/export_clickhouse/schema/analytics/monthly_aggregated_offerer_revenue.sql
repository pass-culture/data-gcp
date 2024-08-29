CREATE OR REPLACE TABLE analytics.monthy_aggregated_offerer_revenue ON cluster default 
    ENGINE = MergeTree
    PARTITION BY creation_month
    ORDER BY (offerer_id)
    SETTINGS storage_policy='gcs_main'
AS

SELECT
    date_trunc('MONTH', toDate (creation_date)) AS creation_month,
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