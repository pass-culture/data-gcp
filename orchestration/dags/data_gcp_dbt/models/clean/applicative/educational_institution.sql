-- TODO: If this model is used in Metabase, put it in the mart folder
SELECT *
FROM {{ ref('int_applicative__educational_institution') }}
