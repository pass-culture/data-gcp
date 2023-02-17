SELECT
    CAST("id" AS varchar(255))
    , CAST("venueId" AS varchar(255)) AS venue_id
    , CAST("pricingPointId" AS varchar(255)) AS pricing_point_id
    , CAST(TRIM(BOTH \'[") \' FROM SPLIT_PART("timespan" :: text, \',\',1)) AS timestamp) AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS pricing_point_link_beginning_date
    , CAST(NULLIF(TRIM(BOTH \'[") \' FROM SPLIT_PART("timespan" :: text, \',\',2)),\'\') AS timestamp) AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS pricing_point_link_ending_date
FROM public.venue_pricing_point_link
