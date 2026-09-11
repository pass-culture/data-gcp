create temp function parse_timestamp(val int64)
as
    (
        case
            when val is null
            then null
            when length(cast(val as string)) = 19
            then timestamp_micros(cast(val / 1000 as int64))
            when length(cast(val as string)) = 16
            then timestamp_micros(val)
            when length(cast(val as string)) = 13
            then timestamp_millis(val)
            when length(cast(val as string)) = 10
            then timestamp_seconds(val)
            else null
        end
    )
;

select
    {% if params.target == "pro" %}
        procedure_id,
        application_id,
        application_number,
        application_archived,
        application_status,

        timestamp_micros(cast((last_update_at) as integer)) as last_update_at,
        timestamp_micros(
            cast((application_submitted_at) as integer)
        ) as application_submitted_at,
        timestamp_micros(
            cast((passed_in_instruction_at) as integer)
        ) as passed_in_instruction_at,
        timestamp_micros(cast((processed_at) as integer)) as processed_at,
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
        parse_timestamp(last_update_at) as last_update_at,
        parse_timestamp(application_submitted_at) as application_submitted_at,
        parse_timestamp(passed_in_instruction_at) as passed_in_instruction_at,
        parse_timestamp(processed_at) as processed_at,
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
