SELECT
     CAST("id" AS varchar(255)) AS venue_registration_id
    , CAST("venueId" AS varchar(255)) AS venue_id
    , "target" AS venue_target
    , "webPresence" AS web_presence
  FROM public.venue_registration
