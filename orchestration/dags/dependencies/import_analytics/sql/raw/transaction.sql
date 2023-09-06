SELECT
    CAST("id" AS varchar(255)),
    CAST("native_transaction_id" AS varchar(255)),
    "issued_at",
    CAST("client_addr" AS varchar(255)),
    CAST("actor_id" AS varchar(255))
FROM public.transaction
