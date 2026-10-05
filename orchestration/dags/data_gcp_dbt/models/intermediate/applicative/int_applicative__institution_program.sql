select ipa.institution_id, ip.program_label as institution_program_name,
from
    {{
        source(
            "raw_eu1",
            "applicative_database_educational_institution_program_association",
        )
    }} as ipa
inner join
    {{ source("raw_eu1", "applicative_database_educational_institution_program") }}
    as ip
    on ipa.program_id = ip.program_id
