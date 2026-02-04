{% snapshot snapshot_raw__venue %}

    {{
        config(
            **custom_snapshot_config(
                strategy="check",
                unique_key="venue_id",
                check_cols=[
                    "venue_name",
                    "venue_siret",
                    "venue_is_permanent",
                    "venue_type_code",
                    "venue_activity",
                    "venue_label_id",
                    "banner_url",
                    "venue_description",
                    "venue_audiodisabilitycompliant",
                    "venue_mentaldisabilitycompliant",
                    "venue_motordisabilitycompliant",
                    "venue_visualdisabilitycompliant",
                    "venue_withdrawal_details",
                ],
            )
        )
    }}

    {% set venue_activity_label_mapping = venue_activity_label() %}
    {% set venue_type_code_label_mapping = venue_type_code_label() %}

    select *
    from
        external_query(
            "{{ env_var('APPLICATIVE_EXTERNAL_CONNECTION_ID') }}",
            """SELECT
            "thumbCount" AS venue_thumb_count
            , CAST("id" AS varchar(255)) AS venue_id
            , "name" AS venue_name
            , "siret" AS venue_siret
            , CAST("managingOffererId" AS varchar(255)) AS venue_managing_offerer_id
            , "bookingEmail" AS venue_booking_email
            , "isVirtual" AS venue_is_virtual
            , "comment" AS venue_comment
            , "publicName" AS venue_public_name
            , {{ render_case_when('"venueTypeCode"', venue_type_code_label_mapping, fallback_sql='"venueTypeCode"') }} as venue_type_code
            , {{ render_case_when('"venue_activity"', venue_activity_label_mapping, fallback_sql='"venue_activity"') }} as venue_activity
            , CAST("venueLabelId" AS varchar(255)) AS venue_label_id
            , "dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS venue_creation_date
            , "isPermanent" AS venue_is_permanent
            , "bannerUrl" as banner_url
            , "audioDisabilityCompliant" AS venue_audioDisabilityCompliant
            , "mentalDisabilityCompliant" AS venue_mentalDisabilityCompliant
            , "motorDisabilityCompliant" AS venue_motorDisabilityCompliant
            , "visualDisabilityCompliant" AS venue_visualDisabilityCompliant
            , "adageId" AS venue_adage_id
            , CAST("venueEducationalStatusId"AS varchar(255)) AS venue_educational_status_id
            , "collectiveDescription" AS collective_description
            , BTRIM(array_to_string("collectiveStudents", \',\'), \'{\') AS collective_students
            , "collectiveWebsite" AS collective_website
            , "collectiveNetwork" AS collective_network
            , "collectiveInterventionArea" AS collective_intervention_area
            , "collectiveAccessInformation" AS collective_access_information
            , "collectivePhone" AS collective_phone
            , "collectiveEmail" AS collective_email
            , "dmsToken" AS dms_token
            , "description" AS venue_description
            , "withdrawalDetails" AS venue_withdrawal_details
        FROM public.venue
    """
        )

{% endsnapshot %}
