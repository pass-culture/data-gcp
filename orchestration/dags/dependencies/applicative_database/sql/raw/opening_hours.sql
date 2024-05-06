SELECT
    CAST("id" AS varchar(255))
    , CAST("venueId" AS varchar(255)) AS venue_id
    , CAST("weekday" AS varchar(255)) AS day_of_the_week
FROM public.opening_hours