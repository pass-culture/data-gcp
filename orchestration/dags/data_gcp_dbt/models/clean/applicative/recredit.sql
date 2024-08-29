select *
from {{ source('raw', 'applicative_database_recredit') }}
