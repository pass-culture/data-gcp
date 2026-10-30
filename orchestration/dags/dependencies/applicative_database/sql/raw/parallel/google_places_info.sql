SELECT
    CAST("id" AS varchar(255)) as google_places_id,
    CAST("venueId" AS varchar(255)) as venue_id
FROM public.google_places_info
