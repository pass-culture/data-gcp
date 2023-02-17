SELECT
    CAST("id" AS varchar(255))
    , CAST("userId" AS varchar(255))
    , "eventType"
    , "eventDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as eventDate
    , cast("actorUserId" AS VARCHAR(255))
    , "reasonCode"
FROM public.user_suspension