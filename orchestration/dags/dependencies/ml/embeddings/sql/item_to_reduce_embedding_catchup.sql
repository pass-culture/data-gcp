SELECT
    ie.item_id,
    ie.name_embedding,
    ie.description_embedding,
    ie.image_embedding,
    ie.semantic_content_embedding,
    ie.semantic_content_hybrid_embedding,
    ie.extraction_date
FROM
    `{{ bigquery_clean_dataset }}`.item_embeddings ie
WHERE
    ie.item_id not in (
        select
            distinct item_id
        from
            `{{ bigquery_clean_dataset }}`.item_embeddings_reduced_5
    ) 
LIMIT {{ params.batch_size }}
