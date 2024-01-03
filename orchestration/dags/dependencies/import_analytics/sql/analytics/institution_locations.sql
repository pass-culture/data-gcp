WITH qpv as (
  SELECT
    code_quartier as code_qpv
    , noms_des_communes_concernees as qpv_name
    , commune_qp as qpv_communes
    , geoshape
  FROM
    `{{ bigquery_analytics_dataset }}.QPV`
  WHERE geoshape is not null
),
institutions as (
  SELECT
    id_etablissement as institution_id
    , nom_etablissement as institution_name
    , nom_commune as institution_city
    , code_postal as institution_postal_code
    , code_departement as institution_department_code
    , cast(latitude as float64) as institution_latitude
    , cast(longitude as float64) as institution_longitude
  FROM
    `{{ bigquery_analytics_dataset }}.eple`
)
SELECT 
  institution_id
  , institution_name
  , institution_city
  , institution_postal_code
  , institution_department_code
  , institution_latitude
  , institution_longitude
  , code_qpv as institution_qpv_code
  , qpv_name as institution_qpv_name
  , CASE
    WHEN code_qpv is null and institution_latitude is null and institution_longitude is null
    THEN null
    ELSE 
    CASE 
        WHEN code_qpv is not null
        THEN 1
        ELSE 0
    END
  END as institution_in_qpv
FROM institutions
LEFT JOIN qpv
ON ST_CONTAINS(
  qpv.geoshape,
  ST_GEOGPOINT(institution_longitude, institution_latitude)
  )
