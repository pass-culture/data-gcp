SELECT
    CAST("id" AS varchar(255))
    , CAST("depositId" AS varchar(255))
    , "amount"
    , "dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as dateCreated
    , "recreditType"
FROM public.recredit