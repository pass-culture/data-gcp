SELECT count(*) as total
FROM `passculture-data-prod.analytics_prod.applicative_database_booking`
WHERE booking_creation_date >= PARSE_DATETIME('%Y%m%d',@DS_START_DATE)    -- Dates à définir sur la dashboard
AND booking_creation_date < PARSE_DATETIME('%Y%m%d',@DS_END_DATE)
