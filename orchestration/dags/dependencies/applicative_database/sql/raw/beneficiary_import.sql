SELECT
    CAST("id" AS varchar(255))
    , CAST("beneficiaryId" AS varchar(255))
    , CAST("applicationId" AS varchar(255))
    , CAST("sourceId" AS varchar(255))
    , "source"
FROM public.beneficiary_import