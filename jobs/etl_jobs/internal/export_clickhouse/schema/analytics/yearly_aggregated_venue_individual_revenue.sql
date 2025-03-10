CREATE OR REPLACE TABLE analytics.yearly_aggregated_venue_individual_revenue ON cluster default
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
            when booking_status = 'USED'
            or booking_status = 'REIMBURSED' then booking_amount*booking_quantity
            else 0
        end
    ) AS individual_revenue
    FROM
        intermediate.booking
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
                when booking_status = 'CONFIRMED'
                then booking_amount*booking_quantity
                else 0
            end
        ) AS individual_expected_revenue
    FROM
        intermediate.booking
    WHERE
        venue_id IS NOT NULL
    GROUP BY 1, 2
)

SELECT
    year,
    venue_id,
    individual_revenue as revenue,
    individual_revenue + individual_expected_revenue AS expected_revenue,
FROM effective_revenue
LEFT JOIN expected_revenue
ON effective_revenue.venue_id = expected_revenue.venue_id
AND effective_revenue.year = expected_revenue.year
