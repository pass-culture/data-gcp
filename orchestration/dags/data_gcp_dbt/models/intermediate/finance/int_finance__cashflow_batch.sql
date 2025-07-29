select id, creationdate, cutoff, label
from {{ source("raw", "applicative_database_cashflow_batch") }}
