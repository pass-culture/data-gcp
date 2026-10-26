select
    procedure_id,
    application_id,
    application_number,
    application_archived,
    application_status,
    instructors,
    demandeur_siret,
    demandeur_naf,
    demandeur_libellenaf,
    demandeur_entreprise_siren,
    demandeur_entreprise_formejuridique,
    demandeur_entreprise_formejuridiquecode,
    demandeur_entreprise_codeeffectifentreprise,
    demandeur_entreprise_raisonsociale,
    demandeur_entreprise_siretsiegesocial,
    statut,
    typologie,
    academie_historique_intervention,
    domaines,
    timestamp_micros(cast(last_update_at as integer)) as last_update_at,
    timestamp_micros(
        cast(application_submitted_at as integer)
    ) as application_submitted_at,
    timestamp_micros(
        cast(passed_in_instruction_at as integer)
    ) as passed_in_instruction_at,
    timestamp_micros(cast(processed_at as integer)) as processed_at,
    case
        when numero_identifiant_lieu like 'PRO-%'
        then trim(numero_identifiant_lieu, 'PRO-')
        else numero_identifiant_lieu
    end as numero_identifiant_lieu,
    case
        when procedure_id = '65028'
        then 'Commission nationale'
        else academie_groupe_instructeur
    end as academie_groupe_instructeur,
    trim(erreur_traitement_pass_culture) as erreur_traitement_pass_culture
from {{ source("raw", "raw_dms_pro") }}
qualify
    row_number() over (
        partition by application_number order by update_date desc, last_update_at desc
    )
    = 1
