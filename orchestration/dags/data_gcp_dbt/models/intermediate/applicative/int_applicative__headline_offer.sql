SELECT 
    id as headline_offer_id
    , offer_id 
    , venue_id 
    , CAST(TRIM(BOTH \'[") \' FROM SPLIT_PART("timespan" :: text, \',\',1)) AS timestamp) AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS headline_beginning_time
    , CAST(NULLIF(TRIM(BOTH \'[") \' FROM SPLIT_PART("timespan" :: text, \',\',2)),\'\') AS timestamp) AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS headline_ending_time
FROM {{ source("raw", "applicative_database_headline_offer") }}