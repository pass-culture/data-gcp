SELECT
    CAST("idAtProvider" AS varchar(255)) as offer_id_at_providers
    , "dateModifiedAtLastProvider" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as offer_modified_at_last_provider_date
    , CAST("id" AS varchar(255)) as offer_id
    , "dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as offer_creation_date
    , CAST("productId" AS varchar(255)) as offer_product_id
    , CAST("venueId" AS varchar(255)) as venue_id
    , CAST("lastProviderId" AS varchar(255)) as offer_last_provider_id
    , "bookingEmail" as booking_email
    , "isActive" as offer_is_active
    , "name" as offer_name
    , "description" as offer_description
    , "conditions" as offer_conditions
    , "ageMin" as offer_age_min
    , "ageMax" as offer_age_max
    , "url" as offer_url
    , "mediaUrls" as offer_media_urls
    , "durationMinutes" as offer_duration_minutes
    , "isNational" as offer_is_national
    , "jsonData" as offer_extra_data
    , "isDuo" as offer_is_duo
    , "fieldsUpdated" as offer_fields_updated
    , "withdrawalDetails" as offer_withdrawal_details
    , "audioDisabilityCompliant" as offer_audio_disability_compliant
    , "mentalDisabilityCompliant" as offer_mental_disability_compliant
    , "motorDisabilityCompliant" as offer_motor_disability_compliant
    , "visualDisabilityCompliant" as offer_visual_disability_compliant
    , "externalTicketOfficeUrl" as offer_external_ticket_office_url
    , CAST("validation" AS varchar(255)) as offer_validation
    , CAST("lastValidationType" AS varchar(255)) as offer_last_validation_type
    , CAST("subcategoryId" AS varchar(255)) as offer_subcategoryId
    , "dateUpdated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as offer_date_updated
    , "withdrawalType" AS offer_withdrawal_type
    , "withdrawalDelay" AS offer_withdrawal_delay
FROM public.offer