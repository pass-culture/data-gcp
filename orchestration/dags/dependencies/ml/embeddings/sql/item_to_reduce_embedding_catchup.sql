SELECT
    ie.item_id AS item_id,
    ie.name AS name,
    ie.description AS description,
    ie.image AS image,
    ie.semantic_content as semantic_content,
    ie.semantic_content_hybrid as semantic_content_hybrid,
    ie.extraction_date as extraction_date
FROM
    `{{ bigquery_clean_dataset }}`.item_embeddings_v4 ie
WHERE
    ie.item_id not in (
        select
            distinct item_id
        from
            `{{ bigquery_clean_dataset }}`.item_embeddings_reduced_5
    ) 
LIMIT {{ params.batch_size }}
