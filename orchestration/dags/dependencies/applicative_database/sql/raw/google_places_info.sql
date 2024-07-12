SELECT
    CAST("ID" AS varchar(255)) as google_places_id,
    CAST("VenueId" AS varchar(255)) as venue_id,
    CAST("BannerUrl" AS varchar(255)) as banner_url
FROM public.google_places_info