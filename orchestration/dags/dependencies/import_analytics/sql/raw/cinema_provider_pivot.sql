SELECT
    CAST("id" AS varchar(255)) AS cinema_provider_pivot_id,
    CAST("venueId" AS varchar(255)) AS venue_id,
    CAST("providerId" AS varchar(255)) AS provider_id,
    CAST("idAtProvider" AS varchar(255)) AS id_at_provider
FROM public.cinema_provider_pivot
