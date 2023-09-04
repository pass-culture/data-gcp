WITH k AS (
    SELECT 
        ie.item_id, 
        ie.name_embedding,
        ie.description_embedding,
        ie.image_embedding
    FROM `{{ bigquery_clean_dataset }}.item_embeddings` ie
    INNER JOIN `{{ bigquery_analytics_dataset }}.recommendable_items_raw` ri on ri.item_id = ie.item_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY item_id ORDER by extraction_date DESC  ) = 1
),

z AS (
    SELECT 
        item_id,
        ARRAY(SELECT cast(e as float64) FROM UNNEST(SPLIT(SUBSTR(name_embedding, 2 , LENGTH(name_embedding) - 2))) e) AS embedding_offer_name,
        ARRAY(SELECT cast(e as float64) FROM UNNEST(SPLIT(SUBSTR(description_embedding, 2 , LENGTH(description_embedding) - 2))) e) AS offer_description_embedding,
        ARRAY(SELECT cast(e as float64) FROM UNNEST(SPLIT(SUBSTR(image_embedding, 2 , LENGTH(image_embedding) - 2))) e) AS offer_image_embedding,
    FROM k
)

SELECT
    item_id, ARRAY_CONCAT(embedding_offer_name, offer_description_embedding, offer_image_embedding) as embedding
FROM z