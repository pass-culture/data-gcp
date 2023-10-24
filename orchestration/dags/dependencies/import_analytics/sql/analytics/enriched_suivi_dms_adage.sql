SELECT
    dms_pro.procedure_id
    , dms_pro.demandeur_entreprise_siren
    , dms_pro.demandeur_siret
    , dms_pro.demandeur_entreprise_siretSiegeSocial AS demandeur_entreprise_siret_siege_social
    , dms_pro.demandeur_entreprise_raisonSociale AS demandeur_entreprise_raison_sociale
    , dms_pro.application_number
    , dms_pro.application_status
    , dms_pro.application_submitted_at
    , dms_pro.passed_in_instruction_at
    , dms_pro.processed_at
    , dms_pro.instructors
    , offerer.offerer_id
    , offerer.offerer_creation_date
    , offerer.offerer_validation_date
    , venue.venue_id
    , CASE WHEN venue.venue_is_permanent THEN CONCAT("venue-",venue.venue_id)
         ELSE CONCAT("offerer-", offerer.offerer_id) END AS partner_id
    , venue.venue_name
    , venue.venue_creation_date
    , venue.venue_is_permanent
    , adage.id as adage_id
    , adage.dateModification as adage_date_modification
    , CASE
        WHEN venue_id IN (
            SELECT
                DISTINCT venueId
            FROM
                `{{ bigquery_clean_dataset }}`.applicative_database_bank_information
        ) 
        THEN TRUE
        ELSE FALSE
    END AS venue_has_bank_information
    , CASE
        WHEN offerer_id IN (
            SELECT
                DISTINCT offererId
            FROM
                `{{ bigquery_clean_dataset }}`.applicative_database_bank_information
            ) 
        THEN TRUE
        ELSE FALSE
    END AS offerer_has_bank_information
    , CASE 
        WHEN venue_id IN (
            SELECT
                venueId
            FROM `{{ bigquery_analytics_dataset }}`.adage
            ) 
        THEN TRUE 
        ELSE FALSE 
    END AS lieu_in_adage
    , CASE 
        WHEN venue_managing_offerer_id IN (
            SELECT
                venue_managing_offerer_id
            FROM `{{ bigquery_analytics_dataset }}`.adage AS adage
            JOIN `{{ bigquery_clean_dataset }}`.applicative_database_venue AS venue 
                ON venue.venue_id = adage.venueId 
            )
        THEN TRUE
        ELSE FALSE
    END AS structure_in_adage
FROM
    `{{ bigquery_clean_dataset }}`.dms_pro_cleaned AS dms_pro
LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_offerer AS offerer 
    ON dms_pro.demandeur_entreprise_siren = offerer.offerer_siren AND offerer.offerer_siren <> "nan"
LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_venue AS venue
    ON venue.venue_managing_offerer_id = offerer.offerer_id 
    AND venue_name != 'Offre numérique'
LEFT JOIN `{{ bigquery_analytics_dataset }}`.adage AS adage 
    ON left(adage.siret, 9) = dms_pro.demandeur_entreprise_siren
WHERE dms_pro.application_status = 'accepte'
AND dms_pro.procedure_id IN ('57081', '57189','61589','65028','80264')
