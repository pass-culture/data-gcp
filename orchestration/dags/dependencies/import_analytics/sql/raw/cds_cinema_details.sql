SELECT
    CAST("id" AS varchar(255)) AS cds_cinema_details_id,
    CAST("cinemaProviderPivotId" AS varchar(255)) AS cinema_provider_pivot_id,
    "cinemaApiToken" AS cinema_api_token,
    "accountId" AS account_id
FROM public.cds_cinema_details
