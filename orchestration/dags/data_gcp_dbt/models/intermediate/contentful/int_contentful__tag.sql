SELECT
    tag_id,
    tag_name,
    entry_id,
    tag_key,
    tag_value,
    CASE
        WHEN tag_key = "pl" THEN tag_value
    END as playlist_type
FROM
    (
        SELECT
            *,
            split(tag_name, ':') [safe_ordinal(1)] as tag_key,
            split(tag_name, ':') [safe_ordinal(2)] as tag_value,
            ROW_NUMBER() OVER (
                PARTITION BY tag_id,
                entry_id
                ORDER BY
                    execution_date DESC
            ) as row_number
        FROM
            FROM {{ source('raw', 'contentful_tags') }}
    ) inn
WHERE
    row_number = 1