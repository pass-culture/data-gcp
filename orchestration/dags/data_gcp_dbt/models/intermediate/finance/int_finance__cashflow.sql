select id, creationdate, status, batchid, amount
from {{ source("raw", "applicative_database_cashflow") }}
