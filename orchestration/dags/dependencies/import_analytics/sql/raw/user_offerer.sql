SELECT
    CAST("id" AS varchar(255))
    , CAST("userId" AS varchar(255))
    , CAST("offererId" AS varchar(255))
    , CAST(CAST("user_offerer"."validationToken" AS varchar(255)) IS NULL AS boolean) AS user_offerer_is_validated
    , CAST("validationStatus" AS varchar(255))  AS user_offerer_validation_status
FROM public.user_offerer