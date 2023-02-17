
SELECT
    CAST(id AS varchar(255)) AS educational_year_id
    , "beginningDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS educational_year_beginning_date
    , "expirationDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS educational_year_expiration_date
    , CAST("adageId" AS varchar(255)) AS adage_id
    , CONCAT(CAST(extract(year from "beginningDate") AS  varchar(255)), \'-\', CAST(extract(year from "expirationDate")AS varchar(255))) AS scholar_year
FROM educational_year