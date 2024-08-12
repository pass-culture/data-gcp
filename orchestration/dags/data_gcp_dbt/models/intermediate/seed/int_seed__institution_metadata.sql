
SELECT
    id_etablissement as institution_id,
    nom_etablissement as institution_name,
    code_postal as institution_postal_code,
    code_departement as institution_department_code,
    CAST(latitude as float64) as institution_latitude,
    CAST(longitude as float64) as institution_longitude
from
    {{ source('seed', 'institution_metadata') }}
