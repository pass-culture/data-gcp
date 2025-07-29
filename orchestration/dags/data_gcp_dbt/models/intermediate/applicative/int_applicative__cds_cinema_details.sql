select cds_cinema_details_id, cinema_provider_pivot_id, cinema_api_token, account_id
from {{ source("raw", "applicative_database_cds_cinema_details") }}
