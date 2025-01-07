with
    dms_applications as (

        select distinct

            procedure_id as dms_application_procedure_id,
            application_id as dms_application_id,
            application_number as dms_application_number,

            cast(
                nullif(application_archived, 'nan') as boolean
            ) as dms_application_archived,
            nullif(application_status, 'nan') as dms_application_status,
            nullif(instructors, '') as dms_application_instructors,

            datetime(
                timestamp_micros(safe_cast((application_submitted_at / 1000) as int64)),
                'Europe/Paris'
            ) as dms_application_submitted_date,
            datetime(
                timestamp_micros(safe_cast((processed_at / 1000) as int64)),
                'Europe/Paris'
            ) as dms_application_processed_date,
            datetime(
                timestamp_micros(safe_cast((passed_in_instruction_at / 1000) as int64)),
                'Europe/Paris'
            ) as dms_application_instruction_passed_date,
            datetime(
                timestamp_micros(safe_cast((last_update_at / 1000) as int64)),
                'Europe/Paris'
            ) as dms_application_last_updated_at,

            -- pro fields only
            nullif(demandeur_siret, 'nan') as dms_application_siret,
            nullif(demandeur_naf, 'nan') as dms_application_naf,
            nullif(demandeur_libellenaf, 'nan') as dms_application_naf_label,
            nullif(demandeur_entreprise_siren, 'nan') as dms_application_company_siren,
            nullif(
                demandeur_entreprise_formejuridique, 'nan'
            ) as dms_application_company_legal_form,
            nullif(
                demandeur_entreprise_formejuridiquecode, 'nan'
            ) as dms_application_company_legal_form_code,
            nullif(
                demandeur_entreprise_raisonsociale, 'nan'
            ) as dms_application_company_corporate_name,
            nullif(
                demandeur_entreprise_siretsiegesocial, 'nan'
            ) as dms_application_company_head_office_siret,
            nullif(
                regexp_replace(numero_identifiant_lieu, 'PRO-', ''), 'nan'
            ) as dms_application_place_identifier_number,
            nullif(statut, 'nan') as dms_application_company_status,
            nullif(typologie, 'nan') as dms_application_company_typology,
            nullif(
                academie_historique_intervention, 'nan'
            ) as dms_application_academic_area_historical_intervention,
            nullif(
                case
                    when procedure_id = '65028'
                    then 'Commission Nationale'
                    else academie_groupe_instructeur
                end,
                'nan'
            ) as dms_application_academic_area_instructor_group,
            nullif(domaines, 'nan') as dms_application_domains,
            nullif(
                erreur_traitement_pass_culture, 'nan'
            ) as dms_application_processing_error_pass_culture

        from {{ source("raw", "raw_dms_pro") }}
        where
            application_submitted_at > 0
            and (processed_at > 0 or processed_at is null)
            and (passed_in_instruction_at > 0 or passed_in_instruction_at is null)

    )

select distinct

    -- identifiers
    dms_application_procedure_id,
    dms_application_id,
    dms_application_number,

    -- application metadata
    first_value(dms_application_archived) over (
        partition by dms_application_number
        order by dms_application_last_updated_at desc
    ) as dms_application_archived,
    first_value(dms_application_status) over (
        partition by dms_application_number
        order by dms_application_last_updated_at desc
    ) as dms_application_status,
    first_value(dms_application_instructors) over (
        partition by dms_application_number
        order by dms_application_last_updated_at desc
    ) as dms_application_instructors,

    -- timestamp
    max(dms_application_last_updated_at) over (
        partition by dms_application_number
    ) as dms_application_last_updated_at,
    min(dms_application_submitted_date) over (
        partition by dms_application_number
    ) as dms_application_submitted_at,
    min(dms_application_processed_date) over (
        partition by dms_application_number
    ) as dms_application_processed_at,
    min(dms_application_instruction_passed_date) over (
        partition by dms_application_number
    ) as dms_application_instruction_passed_at,

    -- dates
    min(date(dms_application_submitted_date)) over (
        partition by dms_application_number
    ) as dms_application_submitted_date,
    min(date(dms_application_processed_date)) over (
        partition by dms_application_number
    ) as dms_application_processed_date,
    min(date(dms_application_instruction_passed_date)) over (
        partition by dms_application_number
    ) as dms_application_instruction_passed_date,
    max(date(dms_application_last_updated_at)) over (
        partition by dms_application_number
    ) as dms_application_last_updated_date,

    -- pro fields only
    {%- for field in [
        "siret",
        "naf",
        "naf_label",
        "company_siren",
        "company_legal_form",
        "company_legal_form_code",
        "company_corporate_name",
        "company_head_office_siret",
        "place_identifier_number",
        "company_status",
        "company_typology",
        "academic_area_historical_intervention",
        "academic_area_instructor_group",
        "domains",
        "processing_error_pass_culture",
    ] %}
        first_value(dms_application_{{ field }}) over (
            partition by dms_application_number
            order by dms_application_last_updated_at desc
        ) as dms_application_{{ field }}
        {% if not loop.last %},{% endif %}
    {%- endfor %}

from dms_applications
order by 8, 9
