select
    id,
    author,
    comment,
    recipientname,
    bookingid,
    amount,
    reimbursementrule,
    transactionendtoendid,
    recipientsiren,
    reimbursementrate,
    transactionlabel,
    paymentmessageid
from {{ source("raw", "applicative_database_payment") }}
