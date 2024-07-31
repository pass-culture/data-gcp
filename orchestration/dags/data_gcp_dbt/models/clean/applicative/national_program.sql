select *
from {{ source('raw', 'applicative_database_national_program') }}
