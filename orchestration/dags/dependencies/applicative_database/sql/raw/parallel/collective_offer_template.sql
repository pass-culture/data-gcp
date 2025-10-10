SELECT
    "audioDisabilityCompliant" AS collective_offer_audio_disability_compliant
    , "mentalDisabilityCompliant" AS collective_offer_mental_disability_compliant
    , "motorDisabilityCompliant" AS collective_offer_motor_disability_compliant
    , "visualDisabilityCompliant" AS collective_offer_visual_disability_compliant
    , "lastValidationDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS collective_offer_last_validation_date
    , CAST("validation" AS VARCHAR) AS collective_offer_validation
    , CAST("id" AS varchar(255)) AS collective_offer_id
    , "isActive" AS collective_offer_is_active
    , CAST("venueId" AS varchar(255)) AS venue_id
    , "name" AS collective_offer_name
    , "description" AS collective_offer_description
    , "durationMinutes" AS collective_offer_duration_minutes
    , "dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS collective_offer_creation_date
    , REPLACE(REPLACE(CAST("formats" AS varchar(255)), \'{\', \'\'), \'}\', \'\')  AS collective_offer_format
    , "dateUpdated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS collective_offer_date_updated
    , BTRIM(array_to_string("students", \',\'), \'{\') AS collective_offer_students
    , "priceDetail" AS collective_offer_price_detail
    , BTRIM(array_to_string("bookingEmails", \',\'), \'{\') AS collective_offer_booking_email
    , BTRIM(array_to_string("interventionArea", \',\'), \'{\') AS intervention_area
    , CAST("lastValidationType" AS VARCHAR) AS collective_offer_last_validation_type
    , CAST("imageId" AS varchar(255)) AS collective_offer_image_id
    , CAST("nationalProgramId" AS varchar(255)) AS national_program_id
    , CAST(TRIM(BOTH \'[") \' FROM SPLIT_PART("dateRange" :: text, \',\',1)) AS timestamp) AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS collective_offer_template_beginning_date
    , CAST(NULLIF(TRIM(BOTH \'[") \' FROM SPLIT_PART("dateRange" :: text, \',\',2)),\'\') AS timestamp) AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS collective_offer_template_ending_date
    , "contactUrl" AS collective_offer_contact_url
    , "contactForm" AS collective_offer_contact_form
    , "contactEmail" AS collective_offer_contact_email
    , "contactPhone" AS collective_offer_contact_phone
    , "rejectionReason" AS collective_offer_rejection_reason
    , "locationType" AS collective_offer_location_type
FROM public.collective_offer_template
