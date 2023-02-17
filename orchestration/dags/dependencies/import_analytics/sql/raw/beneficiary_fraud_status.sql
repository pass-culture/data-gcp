SELECT
    CAST("id" AS varchar(255))
    , CAST("status" AS varchar(255))
    , "date"
    , "detail"
    , CAST("beneficiaryImportId" AS varchar(255))
    , CAST("authorId" AS varchar(255))
FROM public.beneficiary_import_status