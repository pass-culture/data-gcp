SELECT
    {% if params.target == 'pro' %}
    procedure_id,
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
    demandeur_entreprise_siren,
    demandeur_entreprise_formeJuridique,
    demandeur_entreprise_formeJuridiqueCode,
    demandeur_entreprise_codeEffectifEntreprise,
    demandeur_entreprise_raisonSociale,
    demandeur_entreprise_siretSiegeSocial,
    CASE WHEN numero_identifiant_lieu LIKE 'PRO-%' THEN TRIM(numero_identifiant_lieu, 'PRO-')
        ELSE numero_identifiant_lieu END AS numero_identifiant_lieu,
    statut,
    typologie,
    academie_historique_intervention,
    case when procedure_id = '65028' then 'Commission nationale' else groupe_academie_instructeur end as groupe_academie_instructeur,
    domaines,
    TRIM(erreur_traitement_pass_culture) as erreur_traitement_pass_culture
    {% else %}
    procedure_id,
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
    applicant_department,
    applicant_postal_code
    {% endif %}
FROM `{{ bigquery_clean_dataset }}.dms_{{ params.target }}`
QUALIFY ROW_NUMBER() OVER (PARTITION BY application_number ORDER BY update_date DESC, last_update_at DESC) = 1
