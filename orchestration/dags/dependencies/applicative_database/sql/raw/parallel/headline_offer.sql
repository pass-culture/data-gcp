SELECT
     CAST("id" AS varchar(255)) as headline_offer_id
    , CAST("offerId" AS varchar(255)) as offer_id
    , CAST("venueId" AS varchar(255)) as venue_id
    , LOWER("timespan") AS headline_beginning_time
    , UPPER("timespan") AS headline_ending_time
FROM public.headline_offer