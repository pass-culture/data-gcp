SELECT
    CAST("id" AS varchar(255)) as id
    , CAST("userId" AS varchar(255)) as userId
    , CAST("offererId" AS varchar(255)) as offererId
    , CAST("validationStatus" AS varchar(255))  AS user_offerer_validation_status
	, "dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as user_offerer_creation_date
FROM public.user_offerer