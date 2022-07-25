WITH child_tags AS (
  SELECT
    id as child_module_id,
    is_geolocated,
    hits_per_page,
    around_radius,
    REPLACE(tags, '\"', "") AS tag_name
  FROM
    `{{ bigquery_analytics_dataset }}.contentful_entries`,
    UNNEST(JSON_EXTRACT_ARRAY(tags, '$')) AS tags
  where
    tags is not null
),
criterion AS (
  SELECT
    child_module_id,
    is_geolocated,
    hits_per_page,
    around_radius,
    tag_name,
    adc.id as criterion_id,
    adoc.offerId as offer_id,
    ado.offer_name as offer_name
  FROM
    child_tags ct
    LEFT JOIN analytics_prod.applicative_database_criterion adc on adc.name = ct.tag_name
    LEFT JOIN analytics_prod.applicative_database_offer_criterion adoc on adoc.criterionId = adc.id
    LEFT JOIN analytics_prod.applicative_database_offer ado on ado.offer_id = adoc.offerId
),
module_ids AS (
  SELECT
    e.id as module_id,
    e.title as module_name,
    r.child
  FROM
    `{{ bigquery_analytics_dataset }}.contentful_entries` e
    LEFT JOIN `{{ bigquery_analytics_dataset }}.contentful_relationships` r on e.id = r.parent
  WHERE
    e.content_type = "algolia"
)
SELECT
  *
except
(child_module_id, child)
FROM
  module_ids mi
  LEFT JOIN criterion c on c.child_module_id = mi.child