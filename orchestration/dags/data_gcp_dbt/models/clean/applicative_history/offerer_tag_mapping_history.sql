SELECT
    offerer_tag_mapping_id,
    offerer_id,
    tag_id,
    DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) as partition_date
FROM
    {{ source('raw', 'applicative_database_offerer_tag_mapping') }}