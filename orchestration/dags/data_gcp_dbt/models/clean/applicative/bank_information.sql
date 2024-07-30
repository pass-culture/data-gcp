select *
from {{ source('raw', 'applicative_database_bank_information') }}
