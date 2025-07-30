{% snapshot snapshot_raw__collective_offer %}

    {{
        config(
            **custom_snapshot_config(
                strategy="timestamp",
                unique_key="collective_offer_id",
                updated_at="collective_offer_date_updated",
            )
        )
    }}

    select
        collective_offer_audio_disability_compliant,
        collective_offer_mental_disability_compliant,
        collective_offer_motor_disability_compliant,
        collective_offer_visual_disability_compliant,
        collective_offer_last_validation_date,
        collective_offer_validation,
        collective_offer_id,
        collective_offer_is_active,
        venue_id,
        collective_offer_name,
        collective_offer_booking_email,
        collective_offer_description,
        collective_offer_duration_minutes,
        collective_offer_creation_date,
        collective_offer_format,
        collective_offer_students,
        collective_offer_contact_email,
        collective_offer_contact_phone,
        collective_offer_offer_venue,
        collective_offer_venue_humanized_id,
        collective_offer_venue_address_type,
        collective_offer_venue_other_address,
        collective_offer_last_validation_type,
        institution_id,
        intervention_area,
        template_id,
        collective_offer_image_id,
        provider_id,
        national_program_id,
        cast(
            collective_offer_date_updated as timestamp
        ) as collective_offer_date_updated
    from
        external_query(
            "{{ env_var('APPLICATIVE_EXTERNAL_CONNECTION_ID') }}",
            """SELECT
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
        , BTRIM(array_to_string("bookingEmails", \',\'), \'{\') AS collective_offer_booking_email
        , "description" AS collective_offer_description
        , "durationMinutes" AS collective_offer_duration_minutes
        , "dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS collective_offer_creation_date
        , REPLACE(REPLACE(CAST("formats" AS varchar(255)), \'{\', \'\'), \'}\', \'\')  AS collective_offer_format
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
        , CAST("imageId" AS varchar(255)) AS collective_offer_image_id
        , CAST("providerId" AS varchar(255)) AS provider_id
        , CAST("nationalProgramId" AS varchar(255)) AS national_program_id
    FROM public.collective_offer
    """
        )

{% endsnapshot %}
