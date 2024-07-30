with archive_folder_ref as (
    select
        concat(location, collection_id) as archive_full_location,
        slug,
        trim(replace(replace(slug, "___", '_'), '__', '_'), '_') as clean_slug
    from {{ source("raw", "metabase_collection") }}
    where
        concat(location, collection_id) like '/610%'
        and archived = false
),

personal_collection_roots as (
    select distinct concat(location, collection_id) as root
    from {{ source("raw", "metabase_collection") }}
    where personal_owner_id is not null
),

collections_w_root as (
    select
        *,
        concat('/', split(concat(location, collection_id), '/')[safe_offset(1)]) as root
    from {{ source("raw", "metabase_collection") }} c
    where personal_owner_id is null
),

collections_wo_perso as (
    select *
    from collections_w_root
        left join personal_collection_roots
            on collections_w_root.root = personal_collection_roots.root
    where personal_collection_roots.root is null
    order by collection_id

),

object_folder as (
    select
        collection_id,
        concat(location, collection_id) as full_location,
        slug,
        trim(replace(replace(slug, "___", '_'), '__', '_'), '_') as clean_slug,
        array_length(split(concat(location, collection_id), '/')) - 1 location_depth,
        case
            when array_length(split(concat(location, collection_id), '/')) < 3
                then concat(location, collection_id)
            else concat('/', split(concat(location, collection_id), '/')[safe_offset(1)], '/', split(concat(location, collection_id), '/')[safe_offset(2)])
        end as location_reduced_level_2
    from collections_wo_perso
    where
        concat(location, collection_id) not like '/610%'
        and archived = false
        and personal_owner_id is null
),

slug_w_location_reduced as (
    select distinct
        concat(location, collection_id) as full_location,
        slug,
        trim(replace(replace(slug, "___", '_'), '__', '_'), '_') as clean_slug,
        concat(trim(replace(replace(slug, "___", '_'), '__', '_'), '_'), '_archive') as clean_slug_archive
    from collections_wo_perso
    -- 610 is the ID of the "4. Archive" collection. It is being removed here.
    where concat(location, collection_id) not like '/610%'
    -- the collection is not located in Metabase's native archive.
    and archived = false
    -- the collections are public (i.e., remove personal collections)
    and personal_owner_id is null
    -- Filter the collections where the path depth does not exceed 3 elements to retrieve the level 2 parent folder of each card/dashboard.
    -- The archive folder has a maximum depth of 2 folders.
    -- Example: a card to be archived is located in the hierarchy /256/236/159/. Only the first two levels are retrieved
    -- to be able to move the card into the corresponding archive folder: /256/236_archive/159/.
    and array_length(split(concat(location, collection_id), '/')) - 1 < 3
),

archive as (
    select
        concat(location, collection_id) as archive_full_location,
        slug,
        trim(replace(replace(slug, "___", '_'), '__', '_'), '_') as clean_slug,
        case
            when trim(replace(replace(slug, "___", '_'), '__', '_'), '_') in ("1_externe_archive", "2_interne_archive", "3_operationnel_adhoc_archive", "4_archive")
                then trim(replace(replace(slug, "___", '_'), '__', '_'), '_')
            else concat(trim(replace(replace(slug, "___", '_'), '__', '_'), '_'), '_archive')
        end as clean_slug_archive
    from {{ source("raw", "metabase_collection") }}
    where
        concat(location, collection_id) like '/610%'
        and archived = false
)

select
    collection_id,
    object_folder.slug,
    object_folder.clean_slug,
    object_folder.full_location,
    location_reduced_level_2,
    slug_w_location_reduced.slug as slug_reduced_level_2,
    slug_w_location_reduced.clean_slug as clean_slug_reduced_level_2,
    archive_full_location as archive_location_level_2,
    archive.clean_slug as archive_slug_level_2
from object_folder
    left join slug_w_location_reduced
        on object_folder.location_reduced_level_2 = slug_w_location_reduced.full_location
    left join archive
        on archive.clean_slug = slug_w_location_reduced.clean_slug_archive
