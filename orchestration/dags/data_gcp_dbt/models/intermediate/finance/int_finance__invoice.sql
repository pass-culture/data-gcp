select invoice_id, invoice_creation_date, invoice_reference, amount, bank_account_id
from {{ source("raw", "applicative_database_invoice") }}
