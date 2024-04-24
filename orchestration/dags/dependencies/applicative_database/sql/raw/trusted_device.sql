SELECT 
    CAST("userId" AS varchar(255)) AS user_id, 
    "deviceId" AS device_id,
    "source" AS device_source,
    "os" AS device_os,
    "dateCreated"  AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS date_created
FROM public.trusted_device 