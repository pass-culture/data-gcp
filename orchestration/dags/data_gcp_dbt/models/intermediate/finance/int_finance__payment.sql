select
    id,
    author,
    comment,
    recipientName,
    bookingid,
    amount,
    reimbursementRule,
    transactionEndToEndId,
    recipientSiren,
    reimbursementRate,
    transactionLabel,
    paymentMessageid
from {{ source("raw", "applicative_database_payment") }}