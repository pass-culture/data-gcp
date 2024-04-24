WITH
  TEMP AS (
  SELECT
    entry_id,
    COALESCE(title, offer_title) AS bloc_name,
    content_type,
    tag_key,
    tag_value
  FROM
    {{ ref("int_contentful__tag") }} tags
  INNER JOIN
    {{ ref("int_contentful__entry") }} entries 
  ON
    entries.id = tags.entry_id
  WHERE
    content_type != 'homepageNatif' )
SELECT
  entry_id,
  bloc_name,
  content_type,
  ARRAY_TO_STRING(type_playlist, ' , ') AS playlist_type,
  ARRAY_TO_STRING(categorie_offre, ' , ') AS offer_category,
  ARRAY_TO_STRING(playlist_portee, ' , ') AS playlist_reach,
  ARRAY_TO_STRING(playlist_reccurence, ' , ') AS playlist_recurrence
FROM
  TEMP PIVOT ( ARRAY_AGG(tag_value IGNORE NULLS) FOR tag_key IN ('type_playlist',
      'categorie_offre',
      'playlist_portee',
      'playlist_reccurence') )
