SELECT
  entry_id,
  home_name,
  ARRAY_TO_STRING(home_audience, ', ') AS home_audience,
  ARRAY_TO_STRING(home_cycle_vie_utilisateur, ' , ') AS home_cycle_vie_utilisateur,
  ARRAY_TO_STRING(type_home, ', ') AS type_home
FROM (
  SELECT
    entry_id,
    title AS home_name,
    tag_key,
    tag_value
  FROM
    `{{ bigquery_clean_dataset }}.contentful_tags` tags
  INNER JOIN
    `{{ bigquery_raw_dataset }}.contentful_entries` entries
  ON
    entries.id = tags.entry_id
  WHERE
    content_type = 'homepageNatif' ) PIVOT ( ARRAY_AGG(tag_value IGNORE NULLS) FOR tag_key IN ('home_audience',
      'home_cycle_vie_utilisateur',
      'type_home') )