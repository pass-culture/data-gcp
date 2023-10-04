SELECT 
    ,CAST("id" AS VARCHAR(255)) AS ems_cinema_details_id 
    ,CAST("cinemaProviderPivotId" AS VARCHAR(255)) AS cinema_provider_pivot_id
    ,CAST("lastVersion" AS VARCHAR(255)) AS last_version
FROM public.ems_cinema_details