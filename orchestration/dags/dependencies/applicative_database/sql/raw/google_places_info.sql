SELECT
    CAST("id" AS varchar(255)) as google_places_id,
    CAST("venueId" AS varchar(255)) as venue_id,
    CAST("bannerUrl" AS varchar(255)) as banner_url
FROM public.google_places_info