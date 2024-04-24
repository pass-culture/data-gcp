SELECT
    CAST("id" AS varchar(255)) as login_device_history_id
    ,CAST("userId" AS varchar(255)) as user_id
    ,CAST("deviceId" AS varchar(255)) as device_id
    ,"source"
    ,"os"
    ,"location"
    , "dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as login_device_history_timestamp
FROM public.login_device_history