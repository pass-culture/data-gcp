SELECT
    CAST("id" AS varchar(255))
    , "creationDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as creationDate
    , "cutoff"
    ,"label"
FROM public.cashflow_batch
