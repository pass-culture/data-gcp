SELECT
    *
FROM {{ source('raw', 'applicative_database_pricing_line') }}