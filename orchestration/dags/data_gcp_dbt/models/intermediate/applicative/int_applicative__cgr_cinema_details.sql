select
    cgr_cinema_details_id,
    cinema_provider_pivot_id,
    cinema_url
from {{ source("raw", "applicative_database_cgr_cinema_details") }}
