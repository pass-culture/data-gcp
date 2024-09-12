SELECT
    CAST("id" AS varchar(255)) as id
    , CAST("userId" AS varchar(255)) as userId
    , CAST("discordId" AS varchar(255)) as discordId
    , "hasAccess"
    , "isBanned"
    , "lastUpdated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as lastUpdated
FROM public.discord_user