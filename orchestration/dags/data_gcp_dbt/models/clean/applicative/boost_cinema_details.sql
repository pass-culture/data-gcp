SELECT
    *
FROM {{ source('raw', 'applicative_database_boost_cinema_details') }}