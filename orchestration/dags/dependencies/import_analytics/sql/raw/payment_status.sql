SELECT
    CAST("id" AS varchar(255))
    , CAST("paymentId" AS varchar(255))
    , "date" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as date
    , CAST("status" AS varchar(255))
    , "detail"
FROM public.payment_status