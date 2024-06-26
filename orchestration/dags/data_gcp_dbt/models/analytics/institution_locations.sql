WITH qpv AS (
  SELECT
    code_quartier AS code_qpv,
    noms_des_communes_concernees AS qpv_name,
    commune_qp AS qpv_communes,
    geoshape
  FROM
    {{ source('analytics', 'QPV') }}
  WHERE geoshape IS NOT NULL
),
institutions AS (
  SELECT
    id_etablissement AS institution_id,
    nom_etablissement AS institution_name,
    nom_commune AS institution_city,
    code_postal AS institution_postal_code,
    code_departement AS institution_department_code,
    CAST(latitude AS float64) AS institution_latitude,
    CAST(longitude AS float64) AS institution_longitude
  FROM
    {{ source('analytics', 'eple') }}
),
institutions_qpv AS (
  SELECT
    institution_id,
    institution_name,
    institution_city,
    institution_postal_code,
    institution_department_code,
    institution_latitude,
    institution_longitude,
    code_qpv AS institution_qpv_code,
    qpv_name AS institution_qpv_name,
    CASE
      WHEN code_qpv IS NULL AND institution_latitude IS NULL AND institution_longitude IS NULL THEN NULL
      ELSE
        CASE
          WHEN code_qpv IS NOT NULL THEN 1
          ELSE 0
        END
    END AS institution_in_qpv
  FROM institutions
  LEFT JOIN qpv
  ON ST_CONTAINS(
    qpv.geoshape,
    ST_GEOGPOINT(institution_longitude, institution_latitude)
  )
)
SELECT
  iq.*,
  iris_internal_id AS institution_internal_iris_id
FROM institutions_qpv AS iq
LEFT JOIN {{ source('clean', 'geo_iris') }} AS gi
ON ST_CONTAINS(
  gi.iris_shape,
  ST_GEOGPOINT(iq.institution_longitude, iq.institution_latitude)
)