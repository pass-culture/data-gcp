SELECT
    CAST("id" AS varchar(255)) AS recredit_id
    , CAST("depositId" AS varchar(255)) AS deposit_id
    , "amount" AS recredit_amount
    , "dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS recredit_creation_date
    , "recreditType" AS recredit_type
FROM public.recredit
