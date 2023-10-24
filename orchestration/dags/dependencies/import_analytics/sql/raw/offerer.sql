SELECT
    "isActive" AS offerer_is_active
    , "address" AS offerer_address
    , "postalCode" AS offerer_postal_code
    , "city" AS offerer_city
    , CAST("id" AS varchar(255)) AS offerer_id
    , "dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS offerer_creation_date
    , "name" AS offerer_name
    , "siren" AS offerer_siren
    , CAST("validationStatus" as varchar(255)) as offerer_validation_status
    , "dateValidated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS offerer_validation_date
FROM public.offerer