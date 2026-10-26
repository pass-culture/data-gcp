select
    procedure_id,
    application_id,
    application_number,
    application_archived,
    application_status,
    {{ parse_timestamp_dms("last_update_at") }} as last_update_at,
    {{ parse_timestamp_dms("application_submitted_at") }} as application_submitted_at,
    {{ parse_timestamp_dms("passed_in_instruction_at") }} as passed_in_instruction_at,
    {{ parse_timestamp_dms("processed_at") }} as processed_at,
    instructors,
    applicant_department,
    applicant_postal_code
from {{ source("raw", "raw_dms_jeunes") }}
qualify
    row_number() over (
        partition by application_number order by update_date desc, last_update_at desc
    )
    = 1
