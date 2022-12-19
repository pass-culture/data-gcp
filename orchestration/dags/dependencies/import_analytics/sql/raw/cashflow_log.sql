SELECT
    CAST("id" AS varchar(255))
    , CAST("cashflowId" AS varchar(255))
    , "timestamp"
    , "statusBefore"
    , "statusAfter"
    , "details"
FROM public.cashflow_log