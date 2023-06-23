SELECT
    offerer_tag_id,
    offerer_tag_name,
    offerer_tag_label,
    offerer_tag_description,
    DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) as partition_date
FROM
    `{{ bigquery_clean_dataset }}`.`applicative_database_offerer_tag`