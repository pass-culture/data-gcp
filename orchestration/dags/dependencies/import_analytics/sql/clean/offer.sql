WITH offer_rank as (
    SELECT
        *
        , ROW_NUMBER() OVER (PARTITION BY offer_id ORDER BY offer_date_updated DESC) as row_number
    FROM `{{ bigquery_raw_dataset }}.applicative_database_offer`
    WHERE offer_subcategoryid NOT IN ('ACTIVATION_THING', 'ACTIVATION_EVENT')
    AND (
        booking_email != 'jeux-concours@passculture.app'
        OR booking_email IS NULL
    )
)    
SELECT * except(row_number)
FROM offer_rank
WHERE row_number=1