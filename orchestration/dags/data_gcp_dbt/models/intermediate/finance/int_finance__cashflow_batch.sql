select * from {{ source("raw_eu1", "applicative_database_cashflow_batch") }}
