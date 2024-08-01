select
    ipa.institution_id,
    ip.program_label as institution_program_name
from {{ source('raw','applicative_database_educational_institution_program_association') }} as ipa
    inner join {{ source('raw','applicative_database_educational_institution_program') }} as ip
        on ipa.program_id = ip.program_id
