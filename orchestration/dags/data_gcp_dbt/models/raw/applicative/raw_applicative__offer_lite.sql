select
    offer_id_at_providers,
    offer_modified_at_last_provider_date,
    offer_id,
    offer_creation_date,
    offer_product_id,
    venue_id,
    offer_last_provider_id,
    booking_email,
    offer_is_active,
    offer_name,
    offer_description,
    offer_url,
    offer_is_national,
    offer_extra_data,
    offer_ean,
    offer_is_duo,
    offer_fields_updated,
    offer_withdrawal_details,
    offer_audio_disability_compliant,
    offer_mental_disability_compliant,
    offer_motor_disability_compliant,
    offer_visual_disability_compliant,
    offer_external_ticket_office_url,
    offer_validation,
    offer_last_validation_type,
    offer_subcategoryid,
    offer_withdrawal_type,
    offer_withdrawal_delay,
    booking_contact,
    offerer_address_id,
    timestamp(offer_updated_date) as offer_updated_date,
    to_hex(md5(to_json_string(offer))) as custom_scd_id,
    offer_finalization_date,
    offer_publication_date,
    scheduled_offer_bookability_date
from
    external_query(
        "{{ env_var('APPLICATIVE_EXTERNAL_CONNECTION_ID') }}",
        """SELECT
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
        , "url" as offer_url
        , "isNational" as offer_is_national
        , "jsonData" as offer_extra_data
        , "ean" AS offer_ean
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
        , "dateUpdated"  AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as offer_updated_date
        , "withdrawalType" AS offer_withdrawal_type
        , "withdrawalDelay" AS offer_withdrawal_delay
        , CAST("bookingContact" AS varchar(255)) as booking_contact
        , CAST("offererAddressId" AS varchar(255)) as offerer_address_id
        , "finalizationDatetime" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as offer_finalization_date
        , "publicationDatetime" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as offer_publication_date
        , "bookingAllowedDatetime" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as scheduled_offer_bookability_date
    FROM public.offer
    WHERE "dateUpdated" > NOW() - INTERVAL '5' DAY
    """
    ) as offer
