-- TODO check if this query is still useful


WITH child_tags AS (
  SELECT
    id as child_module_id,
    is_geolocated,
    hits_per_page,
    around_radius,
    REPLACE(tags, '\"', "") AS tag_name
  FROM
    {{ ref('int_contentful__entry') }},
    UNNEST(JSON_EXTRACT_ARRAY(tags, '$')) AS tags
  where
    tags is not null 
  and tags != 'nan'
    -- contient uniquement des playlists taggées / exclut les playlists automatiques.
    -- contient envirion 1800 playlists
    -- parmi ces playlists, 30 ont plus d'un tag
),
criterion AS (
  SELECT
    child_module_id,
    is_geolocated,
    hits_per_page,
    around_radius,
    ct.tag_name,
    adc.criterion_id,
    adc.offer_id,
    adc.offer_name
  FROM
    child_tags ct
  LEFT JOIN {{ ref('int_applicative__criterion') }} adc on adc.name = ct.tag_name
),
module_ids AS (
  SELECT
    e.id as module_id,
    e.title as module_name,
    r.child
  FROM
    {{ ref('int_contentful__entry') }} e
    LEFT JOIN {{ ref('int_contentful__relationship') }} r on e.id = r.parent
  WHERE
    e.content_type = "algolia"
)
SELECT
  *
except
(child_module_id, child)
FROM
  module_ids mi
  INNER JOIN criterion c on c.child_module_id = mi.child