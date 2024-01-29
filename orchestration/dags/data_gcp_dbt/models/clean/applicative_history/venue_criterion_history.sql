SELECT 
    DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) as partition_date,
    venue_id,
    venue_criterion_id,
    criterion_id
FROM {{ source('raw', 'applicative_database_venue_criterion') }}