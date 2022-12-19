SELECT
    CAST("id" AS varchar(255))
    , "amount"
    , CAST("userId" AS varchar(255))
    , "source"
    , "dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\'
    , "dateUpdated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\'
    , "expirationDate"
    , "type"
FROM public.deposit