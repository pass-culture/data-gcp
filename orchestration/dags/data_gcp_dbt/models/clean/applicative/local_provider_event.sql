select * from {{ source("raw", "applicative_database_local_provider_event") }}
