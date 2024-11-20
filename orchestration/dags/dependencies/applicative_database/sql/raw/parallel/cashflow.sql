SELECT
    CAST("id" AS varchar(255))
    , "creationDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as creationDate
    , "status"
    , CAST("batchId" AS varchar(255))
    , "amount"
FROM public.cashflow
