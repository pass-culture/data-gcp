-- TODO checkup

-- <TODO> @mripoll why RAW vs Clean here ? 

WITH
  TEMP AS (
  SELECT DISTINCT -- TODO remove as this should not happen anymore
    entry_id,
    COALESCE(title, offer_title) AS bloc_name,
    content_type,
    tag_key,
    tag_value
  FROM
    {{ ref("int_contentful__tags") }} tags
  INNER JOIN
    {{ ref("int_contentful__entries") }} entries 
  ON
    entries.id = tags.entry_id
  WHERE
    content_type != 'homepageNatif' )
SELECT
  entry_id,
  bloc_name,
  content_type,
  ARRAY_TO_STRING(type_playlist, ' , ') AS type_playlist, -- TODO rename
  ARRAY_TO_STRING(categorie_offre, ' , ') AS categorie_offre, -- TODO rename
  ARRAY_TO_STRING(playlist_portee, ' , ') AS playlist_portee, -- TODO rename
  ARRAY_TO_STRING(playlist_reccurence, ' , ') AS playlist_reccurence -- TODO rename
FROM
  TEMP PIVOT ( ARRAY_AGG(tag_value IGNORE NULLS) FOR tag_key IN ('type_playlist',
      'categorie_offre',
      'playlist_portee',
      'playlist_reccurence') )
