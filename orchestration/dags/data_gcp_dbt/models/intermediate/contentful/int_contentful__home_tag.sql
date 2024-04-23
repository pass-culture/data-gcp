WITH
  TEMP AS (
  SELECT DISTINCT
    entry_id,
    title AS home_name,
    tag_key,
    tag_value
  FROM
    {{ ref("int_contentful__tag") }} tags
  INNER JOIN
   {{ ref("int_contentful__entry") }}  entries
  ON
    entries.id = tags.entry_id
  WHERE
    content_type = 'homepageNatif' )
SELECT
  entry_id,
  home_name,
  ARRAY_TO_STRING(home_audience, ', ') AS home_audience, -- TODO rename
  ARRAY_TO_STRING(home_cycle_vie_utilisateur, ' , ') AS home_cycle_vie_utilisateur, -- TODO rename
  ARRAY_TO_STRING(type_home, ', ') AS type_home -- TODO rename
FROM
  TEMP PIVOT ( ARRAY_AGG(tag_value IGNORE NULLS) FOR tag_key IN ('home_audience',
      'home_cycle_vie_utilisateur',
      'type_home') )
