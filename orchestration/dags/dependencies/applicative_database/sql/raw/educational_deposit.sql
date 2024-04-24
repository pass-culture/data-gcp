SELECT
    CAST(id AS varchar(255)) AS educational_deposit_id
    , CAST("educationalInstitutionId" AS varchar(255)) AS educational_institution_id
    , CAST("educationalYearId" AS varchar(255)) AS educational_year_id
    , amount AS educational_deposit_amount
    , "dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS educational_deposit_creation_date
    , CAST("ministry" AS TEXT) AS ministry
FROM educational_deposit


