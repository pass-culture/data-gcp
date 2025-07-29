select boost_cinema_details_id, cinema_provider_pivot_id, cinema_url
from {{ source("raw", "applicative_database_boost_cinema_details") }}
