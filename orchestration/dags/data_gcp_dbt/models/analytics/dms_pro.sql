select
    procedure_id,
    application_id,
    application_number,
    application_archived,
    application_status,
    last_update_at,
    application_submitted_at,
    passed_in_instruction_at,
    processed_at,
    instructors,
    demandeur_siret,
    demandeur_naf,
    demandeur_libellenaf,
    demandeur_entreprise_formejuridique,
    demandeur_entreprise_formejuridiquecode,
    demandeur_entreprise_codeeffectifentreprise,
    demandeur_entreprise_raisonsociale,
    demandeur_entreprise_siretsiegesocial,
    numero_identifiant_lieu,
    statut,
    typologie,
    academie_historique_intervention,
    academie_groupe_instructeur,
    domaines,
    erreur_traitement_pass_culture,
    case
        when (demandeur_entreprise_siren is null or demandeur_entreprise_siren = "nan")
        then left(demandeur_siret, 9)
        else demandeur_entreprise_siren
    end as demandeur_entreprise_siren
from {{ source("clean", "dms_pro_cleaned") }}
