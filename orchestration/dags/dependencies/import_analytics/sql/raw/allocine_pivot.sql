SELECT
    CAST("id" AS varchar(255))
    , CAST("theaterId" AS varchar(255))
    , "internalId" AS internal_id
    , CAST("venueId" AS varchar(255)) AS venue_id
FROM public.allocine_pivot