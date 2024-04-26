SELECT 
    *
FROM {{ source('raw', 'applicative_database_opening_hours') }}