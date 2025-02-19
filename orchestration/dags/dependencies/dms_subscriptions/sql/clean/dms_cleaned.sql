select
    {% if params.target == "pro" %}
        procedure_id,
        application_id,
        application_number,
        application_archived,
        application_status,

        timestamp_micros(cast((last_update_at / 1000) as integer)) as last_update_at,
        timestamp_micros(
            cast((application_submitted_at / 1000) as integer)
        ) as application_submitted_at,
        timestamp_micros(
            cast((passed_in_instruction_at / 1000) as integer)
        ) as passed_in_instruction_at,
        timestamp_micros(cast((processed_at / 1000) as integer)) as processed_at,
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
        case
            when numero_identifiant_lieu like 'PRO-%'
            then trim(numero_identifiant_lieu, 'PRO-')
            else numero_identifiant_lieu
        end as numero_identifiant_lieu,
        statut,
        typologie,
        academie_historique_intervention,
        case
            when procedure_id = '65028'
            then 'Commission nationale'
            else academie_groupe_instructeur
        end as academie_groupe_instructeur,
        domaines,
        trim(erreur_traitement_pass_culture) as erreur_traitement_pass_culture
    {% else %}
        procedure_id,
        application_id,
        application_number,
        application_archived,
        application_status,
        timestamp_micros(cast((last_update_at / 1000) as integer)) as last_update_at,
        timestamp_micros(
            cast((application_submitted_at / 1000) as integer)
        ) as application_submitted_at,
        timestamp_micros(
            cast((passed_in_instruction_at / 1000) as integer)
        ) as passed_in_instruction_at,
        timestamp_micros(cast((processed_at / 1000) as integer)) as processed_at,
        instructors,
        applicant_department,
        applicant_postal_code
    {% endif %}
from `{{ bigquery_raw_dataset }}.raw_dms_{{ params.target }}`
qualify
    row_number() over (
        partition by application_number order by update_date desc, last_update_at desc
    )
    = 1
