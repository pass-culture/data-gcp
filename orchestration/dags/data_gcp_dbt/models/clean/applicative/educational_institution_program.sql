SELECT 
    *
FROM {{ source('raw', 'applicative_database_educational_institution_program') }}
