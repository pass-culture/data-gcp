SELECT
    CAST("id" AS varchar(255))
    , "name"
    , "siret"
    , CAST("bankAccountId" AS varchar(255))
    , "cashflowFrequency"
    , "invoiceFrequency"
    , "status"
FROM public.business_unit