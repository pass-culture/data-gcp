select
    id,
    paymentId,
    date,
    status,
    detail
from {{ source("raw", "applicative_database_payment_status") }}
