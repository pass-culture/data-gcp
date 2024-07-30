WITH archive_folder_ref as
(
    SELECT 
        concat(location, collection_id) as archive_full_location
        , slug
        , trim(replace(replace(slug, "___", '_'), '__', '_'), '_') as clean_slug
    FROM  {{ source("raw", "metabase_collection") }} 
    WHERE concat(location, collection_id) like '/610%'
    AND archived = false
),

personal_collection_roots as (
    SELECT
        distinct concat(location, collection_id) as root
    from   {{ source("raw", "metabase_collection") }} 
    where personal_owner_id is not null
),

collections_w_root as (
  SELECT
        *
        , concat('/', split(concat(location, collection_id), '/')[SAFE_OFFSET(1)]) as root
  FROM  {{ source("raw", "metabase_collection") }} c
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

object_folder as (
    SELECT
      collection_id
      , concat(location, collection_id) as full_location
      , slug
      , trim(replace(replace(slug, "___", '_'), '__', '_'), '_') as clean_slug
      , ARRAY_LENGTH(split(CONCAT(location, collection_id), '/')) - 1 location_depth
      , CASE 
          WHEN ARRAY_LENGTH(split(CONCAT(location, collection_id), '/')) < 3
          THEN concat(location, collection_id)
          ELSE concat('/', split(CONCAT(location, collection_id), '/')[SAFE_OFFSET(1)], '/', split(CONCAT(location, collection_id), '/')[SAFE_OFFSET(2)])
        END as location_reduced_level_2
    FROM collections_wo_perso
    WHERE concat(location, collection_id) not like '/610%'
    AND archived = false
    AND personal_owner_id is null
),

slug_w_location_reduced as (
SELECT DISTINCT
        concat(location, collection_id) as full_location
        , slug
        , trim(replace(replace(slug, "___", '_'), '__', '_'), '_') as clean_slug
        , concat(trim(replace(replace(slug, "___", '_'), '__', '_'), '_'), '_archive') as clean_slug_archive
    FROM collections_wo_perso
    -- 610 is the ID of the "4. Archive" collection. It is being removed here.
    WHERE concat(location, collection_id) not like '/610%'
    -- the collection is not located in Metabase's native archive.
    AND archived = false
    -- the collections are public (i.e., remove personal collections)
    AND personal_owner_id is null
    -- Filter the collections where the path depth does not exceed 3 elements to retrieve the level 2 parent folder of each card/dashboard.
    -- The archive folder has a maximum depth of 2 folders.
    -- Example: a card to be archived is located in the hierarchy /256/236/159/. Only the first two levels are retrieved
    -- to be able to move the card into the corresponding archive folder: /256/236_archive/159/.
    AND ARRAY_LENGTH(split(CONCAT(location, collection_id), '/')) - 1 < 3
),

archive as (
  SELECT 
    concat(location, collection_id) as archive_full_location
    , slug
    , trim(replace(replace(slug, "___", '_'), '__', '_'), '_') as clean_slug
    , CASE
        WHEN trim(replace(replace(slug, "___", '_'), '__', '_'), '_') in ("1_externe_archive", "2_interne_archive", "3_operationnel_adhoc_archive", "4_archive")
        THEN trim(replace(replace(slug, "___", '_'), '__', '_'), '_')
        ELSE concat(trim(replace(replace(slug, "___", '_'), '__', '_'), '_'), '_archive')
    END as clean_slug_archive
  FROM {{ source("raw", "metabase_collection") }} 
  WHERE concat(location, collection_id) like '/610%'
  AND archived = false
)

SELECT
  collection_id
  , object_folder.slug
  , object_folder.clean_slug
  , object_folder.full_location
  , location_reduced_level_2
  , slug_w_location_reduced.slug as slug_reduced_level_2
  , slug_w_location_reduced.clean_slug as clean_slug_reduced_level_2
  , archive_full_location as archive_location_level_2
  , archive.clean_slug as archive_slug_level_2
FROM object_folder
LEFT JOIN slug_w_location_reduced
ON object_folder.location_reduced_level_2 = slug_w_location_reduced.full_location
LEFT JOIN archive
ON archive.clean_slug = slug_w_location_reduced.clean_slug_archive