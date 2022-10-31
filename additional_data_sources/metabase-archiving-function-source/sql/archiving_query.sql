-- Requete Metabase activity modifiée pour retirer les dashboards des collections personnelles et deja presents dans le dossier archive.

WITH personal_collection_roots as (
    SELECT
        distinct concat(location, collection_id) as root
    from  `passculture-data-prod.raw_prod.metabase_collections`
    where personal_owner_id is not null
),

collections_w_root as (
  SELECT
        *
        , concat('/', split(concat(location, collection_id), '/')[SAFE_OFFSET(1)]) as root
  FROM `passculture-data-prod.raw_prod.metabase_collections` c
  WHERE personal_owner_id is null
), 

collections_wo_perso as (
  SELECT * 
  FROM collections_w_root 
  LEFT JOIN personal_collection_roots
  ON collections_w_root.root = personal_collection_roots.root
  WHERE personal_collection_roots.root IS NULL
  ORDER BY COLLECTION_id
), 

dashboard_to_keep as (SELECT
  distinct dashboard_collection_id, id
FROM collections_wo_perso c
JOIN `passculture-data-prod.raw_prod.metabase_report_dashboard` d
ON d.dashboard_collection_id = c.collection_id
WHERE personal_owner_id is null
AND location not like '/610%'
AND c.archived = false)

SELECT 
    ma.card_id
    , ma.dashboard_id
    , ma.context
    , ma.card_name
    , ma.card_collection_id
    , ma.dashboard_name
    , ma.last_execution_date
    , ma.collection_id
    , ma.clean_slug as slug
    , ma.full_location
    , archive_location_level_2
    , archive_slug_level_2
FROM `passculture-data-prod.analytics_prod.metabase_activity` ma
LEFT JOIN dashboard_to_keep
ON ma.dashboard_id = dashboard_to_keep.id
WHERE (all_mates_inactive = 1 and inactive = 1)
OR card_contains_archive = 1
AND (ma.dashboard_id is null OR dashboard_to_keep.id is not null)
AND (ma.location not like '%/607' and ma.location not like '%/606/615') -- remove "1. Externe" et "Adhoc (>1 mois)"