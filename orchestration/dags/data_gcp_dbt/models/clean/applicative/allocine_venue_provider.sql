SELECT
    *
FROM {{ source('raw', 'applicative_database_allocine_venue_provider') }}
