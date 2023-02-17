SELECT
    "isActive"
    , CAST("id" AS varchar(255))
    , CAST("idAtProviders" AS varchar(255))
    , "dateModifiedAtLastProvider" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as dateModifiedAtLastProvider
    , CAST("venueId" AS varchar(255))
    , CAST("providerId" AS varchar(255))
    , CAST("venueIdAtOfferProvider" AS varchar(255))
    , "lastSyncDate"
    , CAST("lastProviderId" AS varchar(255))
    , "fieldsUpdated"
FROM public.venue_provider