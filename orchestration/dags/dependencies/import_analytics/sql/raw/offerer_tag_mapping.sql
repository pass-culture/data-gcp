SELECT
    CAST("id" AS varchar(255)) AS offerer_tag_mapping_id,
    CAST("offererId" AS varchar(255)) AS offerer_id,
    CAST("tagId" AS varchar(255)) AS tag_id
FROM offerer_tag_mapping
