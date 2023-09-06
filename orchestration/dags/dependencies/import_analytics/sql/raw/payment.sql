SELECT
    CAST("id" AS varchar(255)),
    "author",
    "comment",
    "recipientName",
    CAST("bookingId" AS varchar(255)),
    "amount",
    "reimbursementRule",
    CAST("transactionEndToEndId" AS varchar(255)),
    "recipientSiren",
    "reimbursementRate",
    "transactionLabel",
    "paymentMessageId"
FROM public.payment
