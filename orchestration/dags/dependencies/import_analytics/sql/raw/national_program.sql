SELECT
    CAST("id" AS varchar(255)) AS national_program_id
    ,"name" AS national_program_name
    , "dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS national_program_creation_date
 FROM public.national_program


