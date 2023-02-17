SELECT
    CAST("id" AS varchar(255))
    , CAST("pricingId" AS varchar(255))
    , "timestamp"
    , "statusBefore"
    , "statusAfter"
    , "reason"
FROM public.pricing_log