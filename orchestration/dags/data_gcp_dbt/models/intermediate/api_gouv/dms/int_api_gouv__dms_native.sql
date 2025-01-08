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
            regexp_replace(
                nullif(applicant_postal_code, 'nan'), '-', ''
            ) as dms_application_postal_code,

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
            ) as dms_application_last_updated_at

        from {{ source("raw", "raw_dms_jeunes") }}
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
    first_value(dms_application_postal_code) over (
        partition by dms_application_number
        order by dms_application_last_updated_at desc
    ) as dms_application_postal_code,

    -- timestamp
    min(dms_application_submitted_date) over (
        partition by dms_application_number
    ) as dms_application_submitted_at,
    min(dms_application_processed_date) over (
        partition by dms_application_number
    ) as dms_application_processed_at,
    min(dms_application_instruction_passed_date) over (
        partition by dms_application_number
    ) as dms_application_instruction_passed_at,
    max(dms_application_last_updated_at) over (
        partition by dms_application_number
    ) as dms_application_last_updated_at,

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
    ) as dms_application_last_updated_date

from dms_applications
order by 8, 9
