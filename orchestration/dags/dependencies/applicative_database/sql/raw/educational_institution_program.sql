SELECT
     CAST("id" AS varchar(255)) AS program_id
    , "name" AS program_name
    , "label" AS program_label
    , "description" AS program_description
FROM public.educational_institution_program