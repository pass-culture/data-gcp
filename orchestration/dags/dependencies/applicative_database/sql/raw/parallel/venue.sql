SELECT
    "thumbCount" AS venue_thumb_count
    , "street" as venue_street
    , "postalCode" as venue_postal_code
    , "city" as venue_city
    , "banId" as ban_id
    , CAST("id" AS varchar(255)) AS venue_id
    , "name" AS venue_name
    , "siret" AS venue_siret
    , "departementCode" AS venue_department_code
    , "latitude" AS venue_latitude
    , "longitude" AS venue_longitude
    , CAST("managingOffererId" AS varchar(255)) AS venue_managing_offerer_id
    , "bookingEmail" AS venue_booking_email
    , "isVirtual" AS venue_is_virtual
    , "comment" AS venue_comment
    , "publicName" AS venue_public_name
    , CASE
        WHEN "venueTypeCode" = \'ADMINISTRATIVE\' THEN \'Lieu administratif\'
        WHEN "venueTypeCode" = \'DIGITAL\' THEN \'Offre numérique\'
        WHEN "venueTypeCode" = \'BOOKSTORE\' THEN \'Librairie\'
        WHEN "venueTypeCode" = \'PERFORMING_ARTS\' THEN \'Spectacle vivant\'
        WHEN "venueTypeCode" = \'ARTISTIC_COURSE\' THEN \'Cours et pratique artistiques\'
        WHEN "venueTypeCode" = \'MOVIE\' THEN \'Cinéma - Salle de projections\'
        WHEN "venueTypeCode" = \'OTHER\' THEN \'Autre\'
        WHEN "venueTypeCode" = \'CONCERT_HALL\' THEN \'Musique - Salle de concerts\'
        WHEN "venueTypeCode" = \'MUSEUM\' THEN \'Musée\'
        WHEN "venueTypeCode" = \'CULTURAL_CENTRE\' THEN \'Centre culturel\'
        WHEN "venueTypeCode" = \'PATRIMONY_TOURISM\' THEN \'Patrimoine et tourisme\'
        WHEN "venueTypeCode" = \'FESTIVAL\' THEN \'Festival\'
        WHEN "venueTypeCode" = \'MUSICAL_INSTRUMENT_STORE\' THEN \'Musique - Magasin d’instruments\'
        WHEN "venueTypeCode" = \'LIBRARY\' THEN \'Bibliothèque ou médiathèque\'
        WHEN "venueTypeCode" = \'VISUAL_ARTS\' THEN \'Arts visuels, arts plastiques et galeries\'
        WHEN "venueTypeCode" = \'GAMES\' THEN \'Jeux / Jeux vidéos\'
        WHEN "venueTypeCode" = \'CREATIVE_ARTS_STORE\' THEN \'Magasin arts créatifs\'
        WHEN "venueTypeCode" = \'RECORD_STORE\' THEN \'Musique - Disquaire\'
        WHEN "venueTypeCode" = \'SCIENTIFIC_CULTURE\' THEN \'Culture scientifique\'
        WHEN "venueTypeCode" = \'TRAVELING_CINEMA\' THEN \'Cinéma itinérant\'
        WHEN "venueTypeCode" = \'DISTRIBUTION_STORE\' THEN \'Magasin de grande distribution\'
        ELSE "venueTypeCode" END AS venue_type_code
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
    , CAST("offererAddressId"AS varchar(255)) AS offerer_address_id
    , "isOpenToPublic" AS venue_is_open_to_public
    ,"adageInscriptionDate" AS venue_adage_inscription_date
    , "isSoftDeleted" AS venue_is_soft_deleted
FROM public.venue
