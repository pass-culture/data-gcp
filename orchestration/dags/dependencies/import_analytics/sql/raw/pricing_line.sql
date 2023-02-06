SELECT
    CAST("id" AS varchar(255))
    , CAST("pricingId" AS varchar(255))
    , "amount"
    , CAST("category" AS varchar(255))
FROM public.pricing_line