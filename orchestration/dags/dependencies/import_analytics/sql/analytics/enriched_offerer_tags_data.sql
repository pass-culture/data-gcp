SELECT
    offerer_tag_mapping.offerer_tag_mapping_id
    ,offerer_tag_mapping.offerer_id
    , offerer_tag_mapping.tag_id
    ,offerer_tag.offerer_tag_name AS tag_name
    , offerer_tag.offerer_tag_label AS tag_label
    , offerer_tag.offerer_tag_description AS tag_description
    , offerer_tag_category_mapping.offerer_tag_category_id AS tag_category_id
    , offerer_tag_category.offerer_tag_category_name AS tag_category_name
    , offerer_tag_category.offerer_tag_category_label AS tag_category_label
FROM  `{{ bigquery_clean_dataset }}`.applicative_database_offerer_tag_mapping AS offerer_tag_mapping
JOIN `{{ bigquery_clean_dataset }}`.applicative_database_offerer_tag AS offerer_tag
    ON offerer_tag.offerer_tag_id = offerer_tag_mapping.tag_id
JOIN `{{ bigquery_clean_dataset }}`.applicative_database_offerer_tag_category_mapping AS offerer_tag_category_mapping
    ON offerer_tag.offerer_tag_id = offerer_tag_category_mapping.offerer_tag_id
JOIN `{{ bigquery_clean_dataset }}`.applicative_database_offerer_tag_category AS offerer_tag_category
    ON offerer_tag_category_mapping.offerer_tag_category_id = offerer_tag_category.offerer_tag_category_id