SELECT
    CAST("id" AS varchar(255))
    , CAST("userId" AS varchar(255))
    , CAST("offerId" AS varchar(255))
    , CAST("mediationId" AS varchar(255))
    , "dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as dateCreated
FROM public.favorite