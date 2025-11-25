SELECT
    CAST("id" AS varchar(255)) as id
    , CAST("offerId" AS varchar(255)) as offer_id
    , CAST("venueId" AS varchar(255)) as venue_id
    , CAST("timespan" as varchar(255)) as timespan
FROM public.headline_offer
