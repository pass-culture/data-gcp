SELECT
    "thumbCount" AS venue_thumb_count
    , CAST("id" AS varchar(255)) AS venue_id
    , "name" AS venue_name
    , "siret" AS venue_siret
    , CAST("managingOffererId" AS varchar(255)) AS venue_managing_offerer_id
    , "bookingEmail" AS venue_booking_email
    , "isVirtual" AS venue_is_virtual
    , "comment" AS venue_comment
    , "publicName" AS venue_public_name
    , "venueTypeCode" as venue_type_code
    , CAST("activity" AS varchar(255)) AS venue_activity
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
    , "isOpenToPublic" AS venue_is_open_to_public
    , "adageInscriptionDate" AS venue_adage_inscription_date
    , "isSoftDeleted" AS venue_is_soft_deleted
FROM public.venue
