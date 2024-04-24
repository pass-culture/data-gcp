SELECT
    "id" as id
    ,"name" as name
    ,"description" as description
    ,"thumbCount" as thumbCount
    ,"jsonData" as jsonData
    ,"subcategoryId" as subcategoryId
    ,"isGcuCompatible" as isGcuCompatible
    ,"last_30_days_booking" as last_30_days_booking
    ,"lastProviderId" as lastProviderId
    ,"dateModifiedAtLastProvider" as dateModifiedAtLastProvider
    ,"idAtProviders" as idAtProviders
    ,"jsonData" ->> \'ean\' AS ean
FROM public.product
