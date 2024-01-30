SELECT
    *
FROM {{ source('raw', 'applicative_database_allocine_pivot') }}