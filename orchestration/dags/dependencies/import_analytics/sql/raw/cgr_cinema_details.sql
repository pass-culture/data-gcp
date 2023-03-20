SELECT
    CAST("id" AS varchar(255)) AS cgr_cinema_details_id
    , CAST("cinemaProviderPivotId" AS VARCHAR(255)) AS cinema_provider_pivot_id
    , "cinemaUrl" AS cinema_url
FROM public.cgr_cinema_details