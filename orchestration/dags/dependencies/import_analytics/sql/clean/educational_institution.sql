
SELECT
    * except(institution_departement_code),
    COALESCE(
        CASE
            WHEN institution_postal_code = '97150' THEN '978'
            WHEN SUBSTRING(institution_postal_code, 0, 2) = '97' THEN SUBSTRING(institution_postal_code, 0, 3)
            WHEN SUBSTRING(institution_postal_code, 0, 2) = '98' THEN SUBSTRING(institution_postal_code, 0, 3)
            WHEN SUBSTRING(institution_postal_code, 0, 3) in ('200', '201', '209', '205') THEN '2A'
            WHEN SUBSTRING(institution_postal_code, 0, 3) in ('202', '206') THEN '2B'
            ELSE SUBSTRING(institution_postal_code, 0, 2)
        END, 
        institution_departement_code
    ) AS institution_departement_code
FROM `{{ bigquery_raw_dataset }}`.applicative_database_educational_institution
