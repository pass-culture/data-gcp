SELECT
    CAST("id" AS varchar(255))
    , "creationDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\'
    ,"status"
    , CAST("bankAccountId" AS varchar(255))
    , CAST("batchId" AS varchar(255))
    , "amount"
    , CAST("transactionId" AS varchar(255))
FROM public.cashflow