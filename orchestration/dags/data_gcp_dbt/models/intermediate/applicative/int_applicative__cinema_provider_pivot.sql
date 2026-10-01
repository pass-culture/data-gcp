select * from {{ source("raw_eu1", "applicative_database_cinema_provider_pivot") }}
