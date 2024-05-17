SELECT
    ipa.institution_id
    ,ip.program_label AS institution_program_name
FROM {{ source('raw','applicative_database_educational_institution_program_association') }} AS ipa
INNER JOIN {{ source('raw','applicative_database_educational_institution_program') }} AS ip
    ipa.program_id = ip.program_id
