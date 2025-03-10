CREATE OR REPLACE TABLE analytics.yearly_aggregated_venue_collective_revenue ON cluster default
    ENGINE = SummingMergeTree()
    PARTITION BY year
    ORDER BY (venue_id)
    SETTINGS storage_policy='gcs_main'
AS
WITH effective_revenue as (
    SELECT
        toStartOfYear(coalesce(toDate(used_date), toDate(creation_date))) AS year,
        cast(venue_id as String) as venue_id,
        sum(
            case
                when collective_booking_status = 'USED'
                or collective_booking_status = 'REIMBURSED' then booking_amount
                else 0
            end
        ) AS collective_revenue
    FROM
        intermediate.collective_booking
    WHERE
        venue_id IS NOT NULL
    GROUP BY 1,2
)
, expected_revenue as (
    SELECT
        toStartOfYear(toDate(now())) AS year,
        cast(venue_id as String) as venue_id,
        sum(
            case
                when collective_booking_status = 'CONFIRMED'
                then booking_amount
                else 0
            end
        ) AS collective_expected_revenue
    FROM
        intermediate.collective_booking
    WHERE
        venue_id IS NOT NULL
    GROUP BY 1, 2
)
SELECT
    year,
    venue_id,
    collective_revenue as revenue,
    collective_revenue + collective_expected_revenue as expected_revenue
FROM effective_revenue
LEFT JOIN expected_revenue
ON effective_revenue.venue_id = expected_revenue.venue_id
AND effective_revenue.year = expected_revenue.year
