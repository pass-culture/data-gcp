SELECT
    "id" as id
    ,"name" as name
    ,"description" as description
    ,"thumbCount" as thumbCount
    ,"extraData" as extraData
    ,"subcategoryId" as subcategoryId
    ,"isGcuCompatible" as isGcuCompatible
    ,"isSynchronizationCompatible" as isSynchronizationCompatible
    ,"last_30_days_booking" as last_30_days_booking
    ,"lastProviderId" as lastProviderId
    ,"dateModifiedAtLastProvider" as dateModifiedAtLastProvider
    ,"idAtProviders" as idAtProviders
FROM public.product