SELECT
    "thumbCount"
    , CAST("idAtProviders" AS varchar(255))
    , "dateModifiedAtLastProvider"
    , CAST("id" AS varchar(255))
    , "dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as dateCreated
    , CAST("authorId" AS varchar(255))
    , CAST("lastProviderId" AS varchar(255))
    , CAST("offerId" AS varchar(255))
    , "credit"
    , "isActive"
    , "fieldsUpdated"
FROM public.mediation