SELECT
    dms_pro.procedure_id,
    dms_pro.demandeur_entreprise_siren,
    dms_pro.demandeur_siret,
    dms_pro.demandeur_entreprise_siretSiegeSocial AS demandeur_entreprise_siret_siege_social,
    dms_pro.demandeur_entreprise_raisonSociale AS demandeur_entreprise_raison_sociale,
    dms_pro.application_number,
    dms_pro.application_status,
    dms_pro.application_submitted_at,
    dms_pro.passed_in_instruction_at,
    dms_pro.processed_at,
    dms_pro.instructors,
    enriched_offerer.offerer_id,
    enriched_offerer.offerer_creation_date,
    enriched_offerer.offerer_validation_date,
    enriched_venue.venue_id,
    enriched_venue.venue_name,
    enriched_venue.venue_creation_date,
    adage.id as adage_id,
    adage.dateModification as adage_date_modification,
    CASE
        WHEN venue_id IN (
            SELECT
                DISTINCT venueId
            FROM
                `{{ bigquery_analytics_dataset }}`.applicative_database_bank_information
        ) THEN TRUE
        ELSE FALSE
    END AS venue_has_bank_information,
    CASE
        WHEN offerer_id IN (
            SELECT
                DISTINCT offererId
            FROM
                `{{ bigquery_analytics_dataset }}`.applicative_database_bank_information
        ) THEN TRUE
        ELSE FALSE
    END AS offerer_has_bank_information,
    CASE WHEN venue_id IN (
        SELECT
            venueId
        FROM `{{ bigquery_analytics_dataset }}`.adage ) THEN TRUE ELSE FALSE END AS lieu_in_adage,
    CASE WHEN venue_managing_offerer_id IN (
        SELECT
            venue_managing_offerer_id
        FROM `{{ bigquery_analytics_dataset }}`.adage AS adage
        JOIN `{{ bigquery_analytics_dataset }}`.enriched_venue_data AS enriched_venue_data ON enriched_venue_data.venue_id = adage.venueId )
                                                        THEN TRUE ELSE FALSE END AS structure_in_adage
    , typeform.token AS typeform_token
    , typeform.vous_etes AS typeform_applicant_status
    , typeform.quels_sont_vos_domaines_d_intervention AS typeform_applicant_intervention_domain
    , typeform.quel_est_votre_type_de_structure AS typeform_applicant_type
FROM
    `{{ bigquery_analytics_dataset }}`.dms_pro AS dms_pro
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.enriched_offerer_data AS enriched_offerer ON dms_pro.demandeur_entreprise_siren = enriched_offerer.offerer_siren
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.enriched_venue_data AS enriched_venue ON enriched_venue.venue_managing_offerer_id = enriched_offerer.offerer_id
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.adage AS adage ON adage.siret = dms_pro.demandeur_siret
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.typeform_adage_reference_request typeform ON typeform.quel_est_le_numero_de_siret_de_votre_structure = dms_pro.demandeur_siret
WHERE dms_pro.application_status = 'accepte'