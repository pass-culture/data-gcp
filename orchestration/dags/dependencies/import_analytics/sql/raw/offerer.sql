SELECT
    "isActive" AS offerer_is_active
    , "thumbCount" AS offerer_thumb_count
    , CAST("idAtProviders" AS varchar(255)) AS offerer_id_at_providers
    , "dateModifiedAtLastProvider" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS offerer_modified_at_last_provider_date
    , "address" AS offerer_address
    , "postalCode" AS offerer_postal_code
    , "city" AS offerer_city
    , CAST("id" AS varchar(255)) AS offerer_id
    , "dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS offerer_creation_date
    , "name" AS offerer_name
    , "siren" AS offerer_siren
    , CAST("lastProviderId" AS varchar(255)) AS offerer_last_provider_id
    , "fieldsUpdated" AS offerer_fields_updated
    , CAST("validationStatus" as varchar(255)) as offerer_validation_status
    , "dateValidated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS offerer_validation_date
FROM public.offerer