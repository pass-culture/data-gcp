SELECT
    CAST("id" AS varchar(255))
    , CAST("venueId" AS varchar(255)) AS venue_id
    , CAST("weekday" AS varchar(255)) AS day_of_the_week
    , CAST(TRIM(BOTH \'[") \' FROM SPLIT_PART("timespan" :: text, \',\',1)) AS timestamp) AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS opening_beginning_date
    , CAST(NULLIF(TRIM(BOTH \'[") \' FROM SPLIT_PART("timespan" :: text, \',\',2)),\'\') AS timestamp) AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS opening_ending_date
FROM public.opening_hours