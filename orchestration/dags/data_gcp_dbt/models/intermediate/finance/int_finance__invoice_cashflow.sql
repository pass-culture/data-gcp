select * from {{ source("raw_eu1", "applicative_database_invoice_cashflow") }}
