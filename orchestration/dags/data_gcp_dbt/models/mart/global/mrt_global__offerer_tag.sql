SELECT
    otm.offerer_tag_mapping_id,
    otm.offerer_id,
    otm.tag_id,
    ot.offerer_tag_name AS tag_name,
    ot.offerer_tag_label AS tag_label,
    ot.offerer_tag_description AS tag_description,
    otcm.offerer_tag_category_id AS tag_category_id,
    otc.offerer_tag_category_name AS tag_category_name,
    otc.offerer_tag_category_label AS tag_category_label,
FROM  {{ source('raw', 'applicative_database_offerer_tag_mapping') }} AS otm
INNER JOIN {{ source('raw', 'applicative_database_offerer_tag') }} AS ot
    ON ot.offerer_tag_id = otm.tag_id
INNER JOIN {{ source('raw', 'applicative_database_offerer_tag_category_mapping') }} AS otcm
    ON ot.offerer_tag_id = otcm.offerer_tag_id
INNER JOIN {{ source('raw', 'applicative_database_offerer_tag_category') }} AS otc
    ON otcm.offerer_tag_category_id = otc.offerer_tag_category_id
