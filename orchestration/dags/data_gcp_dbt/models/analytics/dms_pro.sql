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
        THEN LEFT(demandeur_siret, 9) ELSE demandeur_entreprise_siren END AS demandeur_entreprise_siren
FROM {{ source('clean','dms_pro_cleaned') }}