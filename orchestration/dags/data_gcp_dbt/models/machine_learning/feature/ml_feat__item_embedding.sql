

SELECT
    ie.item_id,
    ie.image_embedding,
    ie.name_embedding,
    ie.description_embedding,
    ie.semantic_content_embedding,
    ie.semantic_content_hybrid_embedding,
    ie.label_embedding,
    ie.label_hybrid_embedding,
    ie.extraction_date,
    ie.extraction_datetime,
FROM
    {{ source('ml_preproc', 'item_embedding_extraction') }} ie
INNER JOIN {{ ref("item_metadata") }} im on ie.item_id = im.item_id

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ie.item_id
    ORDER by
        ie.extraction_datetime DESC
) = 1
