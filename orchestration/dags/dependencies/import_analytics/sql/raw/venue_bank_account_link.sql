SELECT 
    CAST("id" AS VARCHAR(255)) AS bank_account_link_id
    ,CAST("VenueId" AS VARCHAR(255)) AS venue_id 
    ,CAST("BankAccountId" AS VARCHAR(255)) AS bank_account_id
    ,CAST(TRIM(BOTH \'[") \' FROM SPLIT_PART("timespan" :: text, \',\',1)) AS timestamp) AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS bank_account_link_beginning_date
    ,CAST(NULLIF(TRIM(BOTH \'[") \' FROM SPLIT_PART("timespan" :: text, \',\',2)),\'\') AS timestamp) AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS bank_account_link_ending_date
FROM public.venue_bank_account_link