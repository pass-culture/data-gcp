SELECT
    CAST("id" AS varchar(255)) as id
    , "amount"
    , CAST("userId" AS varchar(255)) as userId
    , "source"
    , "dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as dateCreated
    , "dateUpdated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as dateUpdated
    , "expirationDate"
    , "type"
FROM public.deposit