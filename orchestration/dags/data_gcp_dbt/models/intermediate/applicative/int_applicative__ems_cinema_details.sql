select ems_cinema_details_id, cinema_provider_pivot_id, last_version
from {{ source("raw", "applicative_database_ems_cinema_details") }}
