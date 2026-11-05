SELECT
    CAST("id" AS varchar(255)) as google_places_id,
    CAST("venueId" AS varchar(255)) as venue_id,
    CAST("bannerId" AS varchar(255)) as banner_id
FROM public.google_places_info
