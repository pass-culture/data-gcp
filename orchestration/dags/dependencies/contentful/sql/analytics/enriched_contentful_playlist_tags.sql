WITH
  TEMP AS (
  SELECT DISTINCT
    entry_id,
    COALESCE(title, offer_title) AS bloc_name,
    content_type,
    tag_key,
    tag_value
  FROM
    `{{ bigquery_clean_dataset }}.contentful_tags` tags
  INNER JOIN
    `{{ bigquery_raw_dataset }}.contentful_entries` entries
  ON
    entries.id = tags.entry_id
  WHERE
    content_type != 'homepageNatif' )
SELECT
  entry_id,
  bloc_name,
  content_type,
  ARRAY_TO_STRING(type_playlist, ' , ') AS type_playlist,
  ARRAY_TO_STRING(categorie_offre, ' , ') AS categorie_offre,
  ARRAY_TO_STRING(playlist_portee, ' , ') AS playlist_portee,
  ARRAY_TO_STRING(playlist_reccurence, ' , ') AS playlist_reccurence
FROM
  TEMP PIVOT ( ARRAY_AGG(tag_value IGNORE NULLS) FOR tag_key IN ('type_playlist',
      'categorie_offre',
      'playlist_portee',
      'playlist_reccurence') )
