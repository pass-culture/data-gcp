select
    cinema_provider_pivot_id,
    venue_id,
    provider_id,
    id_at_provider
from {{ source("raw", "applicative_database_cinema_provider_pivot") }}