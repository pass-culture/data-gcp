SELECT
     CAST("id" AS varchar(255)) as headline_offer_id
    , CAST("offerId" AS varchar(255)) as offer_id
    , CAST("venueId" AS varchar(255)) as venue_id
    , CAST(TRIM(BOTH \'[") \' FROM SPLIT_PART("timespan" :: text, \',\',1)) AS timestamp) AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS headline_beginning_time
    , CAST(NULLIF(TRIM(BOTH \'[") \' FROM SPLIT_PART("timespan" :: text, \',\',2)),\'\') AS timestamp) AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS headline_ending_time
FROM public.headline_offer