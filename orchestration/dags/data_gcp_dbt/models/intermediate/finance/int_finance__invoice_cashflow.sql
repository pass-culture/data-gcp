select
    invoice_id,
    cashflow_id
from {{ source("raw", "applicative_database_invoice_cashflow") }}
