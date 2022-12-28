SELECT
    CAST("id" AS varchar(255))
    , CAST("providerId" AS varchar(255))
    , "date" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as date 
    , CAST("type" AS varchar(255))
    , "payload"
FROM public.local_provider_event