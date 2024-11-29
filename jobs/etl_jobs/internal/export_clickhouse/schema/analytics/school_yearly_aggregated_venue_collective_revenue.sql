CREATE OR REPLACE TABLE analytics.school_yearly_aggregated_venue_collective_revenue ON cluster default
    ENGINE = SummingMergeTree()
    PARTITION BY scholar_year
    ORDER BY (venue_id)
    SETTINGS storage_policy='gcs_main'
AS
SELECT
    -- Calculate scholar year: Aug to Jul is treated as the same "scholar year"
    concat(
        toString(
            if(
                extract('month', toDate(creation_date)) >= 8,
                extract('year', toDate(creation_date)),
                extract('year', toDate(creation_date)) - 1
            )
        ),
        '-',
        toString(
            if(
                extract('month', toDate(creation_date)) >= 8,
                extract('year', toDate(creation_date)) + 1,
                extract('year', toDate(creation_date))
            )
        )
    ) AS scholar_year,
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
GROUP BY 1, 2
