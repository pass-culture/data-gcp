SELECT
    *
FROM {{ source('raw', 'applicative_database_collective_stock') }}
