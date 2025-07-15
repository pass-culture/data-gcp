SELECT
    "id" as id
    ,"name" as name
    ,"description" as description
    ,"thumbCount" as thumbCount
    ,"jsonData" as jsonData
    ,"subcategoryId" as subcategoryId
    ,"last_30_days_booking" as last_30_days_booking
    ,"lastProviderId" as lastProviderId
    ,"dateModifiedAtLastProvider" as dateModifiedAtLastProvider
    ,"ean" AS ean
    ,"gcuCompatibilityType" AS gcuCompatibilityType
FROM public.product
