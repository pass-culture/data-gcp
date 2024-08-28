SELECT procedure_id,
    application_id,
    application_number,
    application_archived,
    application_status,
    last_update_at,
    application_submitted_at,
    passed_in_instruction_at,
    processed_at,
    application_motivation,
    instructors,
    demandeur_siret,
    demandeur_naf,
    demandeur_libelleNaf,
    demandeur_entreprise_formeJuridique,
    demandeur_entreprise_formeJuridiqueCode,
    demandeur_entreprise_codeEffectifEntreprise,
    demandeur_entreprise_raisonSociale,
    demandeur_entreprise_siretSiegeSocial,
    numero_identifiant_lieu,
    statut,
    typologie,
    academie_historique_intervention,
    academie_groupe_instructeur,
    domaines,
    erreur_traitement_pass_culture,
    CASE WHEN (demandeur_entreprise_siren IS NULL OR demandeur_entreprise_siren = "nan")
        THEN LEFT(demandeur_siret, 9) ELSE demandeur_entreprise_siren END AS demandeur_entreprise_siren,


    CASE WHEN siret_synchro_adage AND a.siret IS NOT NULL THEN TRUE ELSE FALSE END AS siret_synchro_adage,

    CASE WHEN demandeur_entreprise_siren IN (SELECT siren from siren_reference_adage WHERE siren_synchro_adage) THEN TRUE ELSE FALSE END AS siren_synchro_adage,






    CASE WHEN a.siret IS NOT NULL THEN TRUE ELSE FALSE END AS siret_ref_adage,

    CASE WHEN siret_synchro_adage AND a.siret IS NOT NULL THEN TRUE ELSE FALSE END AS siret_synchro_adage,

    case when demandeur_entreprise_siren in (select siren from siren_reference_adage) then TRUE else FALSE end as siren_ref_adage,
    case when demandeur_entreprise_siren in (select siren from siren_reference_adage where siren_synchro_adage) then TRUE else FALSE end as siren_synchro_adage

FROM {{ source('clean','dms_pro_cleaned') }} AS dpc
LEFT JOIN {{ ref('adage') }} AS a ON dpc.demandeur_siret = a.siret
