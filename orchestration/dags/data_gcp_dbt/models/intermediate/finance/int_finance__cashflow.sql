select
    id,
    creationdate,
    status,
    batchId,
    amount
from {{ source("raw", "applicative_database_cashflow") }}
