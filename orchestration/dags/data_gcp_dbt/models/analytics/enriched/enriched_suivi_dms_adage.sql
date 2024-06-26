WITH siren_reference_adage AS (
  SELECT 
    siren,
    max(siren_synchro_adage) AS siren_synchro_adage
  FROM {{ ref('adage') }}
  GROUP BY 1
)

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
    , CASE WHEN demandeur_siret IN (SELECT siret from {{ ref('adage') }}) THEN TRUE ELSE FALSE END AS siret_ref_adage
    , CASE WHEN demandeur_siret IN (SELECT siret from {{ ref('adage') }} where siret_synchro_adage = TRUE) THEN TRUE ELSE FALSE END AS siret_synchro_adage
    , CASE WHEN demandeur_entreprise_siren IN (SELECT siren from siren_reference_adage) THEN TRUE ELSE FALSE END AS siren_ref_adage
    , CASE WHEN demandeur_entreprise_siren IN (SELECT siren from siren_reference_adage WHERE siren_synchro_adage) THEN TRUE ELSE FALSE END AS siren_synchro_adage

FROM
    {{ ref('dms_pro') }}
LEFT JOIN {{ ref('offerer') }} AS offerer
    ON dms_pro.demandeur_entreprise_siren = offerer.offerer_siren AND offerer.offerer_siren <> "nan"
LEFT JOIN {{ ref('venue') }} AS venue
    ON venue.venue_managing_offerer_id = offerer.offerer_id 
    AND venue_name != 'Offre numérique'
LEFT JOIN {{ source('analytics','adage') }} AS adage
    ON adage.siret = dms_pro.demandeur_siret
WHERE dms_pro.application_status = 'accepte'
AND dms_pro.procedure_id IN ('57081', '57189','61589','65028','80264')