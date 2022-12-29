SELECT
    "audioDisabilityCompliant" AS collective_offer_audio_disability_compliant
    , "mentalDisabilityCompliant" AS collective_offer_mental_disability_compliant
    , "motorDisabilityCompliant" AS collective_offer_motor_disability_compliant
    , "visualDisabilityCompliant" AS collective_offer_visual_disability_compliant
    , "lastValidationDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS collective_offer_last_validation_date
    , CAST("validation" AS VARCHAR) AS collective_offer_validation
    , CAST("id" AS varchar(255)) AS collective_offer_id
    , CAST("offerId" AS varchar(255)) AS offer_id
    , "isActive" AS collective_offer_is_active
    , CAST("venueId" AS varchar(255)) AS venue_id
    , "name" AS collective_offer_name
    , BTRIM(array_to_string("bookingEmails", \',\'), \'{\') AS collective_offer_booking_email
    , "description" AS collective_offer_description
    , "durationMinutes" AS collective_offer_duration_minutes
    , "dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS collective_offer_creation_date
    , CAST("subcategoryId" AS varchar(255)) AS collective_offer_subcategory_id
    , "dateUpdated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS collective_offer_date_updated
    , BTRIM(array_to_string("students", \',\'), \'{\') AS collective_offer_students
    , "contactEmail" AS collective_offer_contact_email
    , "contactPhone" AS collective_offer_contact_phone
    , "offerVenue" AS collective_offer_offer_venue
    , "offerVenue" ->> \'venueId\' AS collective_offer_venue_humanized_id
    , "offerVenue" ->> \'addressType\' AS collective_offer_venue_address_type
    , "offerVenue" ->> \'otherAddress\' AS collective_offer_venue_other_address
    , CAST("lastValidationType" AS VARCHAR) AS collective_offer_last_validation_type
    , CAST("institutionId" AS varchar(255)) AS institution_id
    , BTRIM(array_to_string("interventionArea", \',\'), \'{\') AS intervention_area
    , CAST("templateId" AS varchar(255)) AS template_id
FROM public.collective_offer
