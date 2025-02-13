SELECT
     CAST("id" AS varchar(255)) as headline_offer_id
    , CAST("offerId" AS varchar(255)) as offer_id
    , CAST("venueId" AS varchar(255)) as venue_id
    , TIMESTAMP(REGEXP_REPLACE(SPLIT(timespan, ',')[SAFE_OFFSET(0)], r'[\[\]"]', '')) AS headline_beginning_time
    , TIMESTAMP(NULLIF(REGEXP_REPLACE(SPLIT(timespan, ',')[SAFE_OFFSET(1)], r'[\[\]"]', ''), '')) AS headline_ending_time
FROM public.headline_offer