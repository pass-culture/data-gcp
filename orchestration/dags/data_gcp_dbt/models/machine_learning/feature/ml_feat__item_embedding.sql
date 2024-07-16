

SELECT
    item_id,
    image_embedding,
    name_embedding,
    description_embedding,
    semantic_content_embedding,
    semantic_content_hybrid_embedding,
    label_embedding,
    label_hybrid_embedding,
    extraction_date,
    extraction_datetime,
FROM
    {{ source('ml_preproc', 'item_embedding_extraction') }}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY item_id
    ORDER by
        extraction_datetime DESC
) = 1
