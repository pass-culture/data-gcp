SELECT
    CAST(id AS varchar(255)) AS educational_institution_id
    , CAST("institutionId" AS varchar(255)) AS institution_id
    , "city" AS institution_city
    , "name" AS institution_name 
    , "postalCode" AS institution_postal_code
    , CASE
        WHEN ("postalCode" LIKE \'97%\' OR "postalCode" LIKE \'98%\') THEN SUBSTRING("postalCode",1,3) 
        ELSE SUBSTRING("postalCode",1,2) END AS institution_departement_code
    , "institutionType" AS institution_type
FROM educational_institution