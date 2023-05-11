
SELECT
    * except(venue_department_code),
    COALESCE(CASE
        WHEN SUBSTRING(venue_postal_code, 0, 2) = '97' THEN SUBSTRING(venue_postal_code, 0, 3)
        WHEN SUBSTRING(venue_postal_code, 0, 2) = '98' THEN SUBSTRING(venue_postal_code, 0, 3)
        WHEN SUBSTRING(venue_postal_code, 0, 3) in ('200', '201', '209', '205') THEN '2A'
        WHEN SUBSTRING(venue_postal_code, 0, 3) in ('202', '206') THEN '2B'
        ELSE SUBSTRING(venue_postal_code, 0, 2)
        END, 
        venue_department_code
    ) AS venue_department_code
FROM `{{ bigquery_raw_dataset }}`.applicative_database_venue
