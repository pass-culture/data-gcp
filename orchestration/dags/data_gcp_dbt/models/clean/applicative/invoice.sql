SELECT 
    *
FROM {{ source('raw', 'applicative_database_invoice') }}