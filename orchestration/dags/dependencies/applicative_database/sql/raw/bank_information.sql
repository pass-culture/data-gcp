SELECT
    CAST("id" AS varchar(255))
    , CAST("offererId" AS varchar(255))
    , CAST("venueId" AS varchar(255))
    , CAST("applicationId" AS varchar(255))
    , "dateModified" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as dateModified
    , CAST("status" AS varchar(255))
FROM public.bank_information