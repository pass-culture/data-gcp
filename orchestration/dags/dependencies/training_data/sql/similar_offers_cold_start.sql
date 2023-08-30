WITH k AS (
    SELECT 
        ie.item_id, 
        ie.offer_name_embedding,
        ie.offer_description_embedding,
        ie.offer_image_embedding
    FROM `{{ bigquery_clean_dataset }}.item_embeddings` ie
    INNER JOIN `{{ bigquery_analytics_dataset }}.recommendable_items_raw` ri on ri.item_id = ie.item_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY item_id ORDER by extraction_date DESC  ) = 1
),

z AS (
    SELECT 
        item_id,
        ARRAY(SELECT cast(e as float64) FROM UNNEST(SPLIT(SUBSTR(offer_name_embedding, 2 , LENGTH(offer_name_embedding) - 2))) e) AS embedding_offer_name,
        ARRAY(SELECT cast(e as float64) FROM UNNEST(SPLIT(SUBSTR(offer_description_embedding, 2 , LENGTH(offer_description_embedding) - 2))) e) AS offer_description_embedding,
        ARRAY(SELECT cast(e as float64) FROM UNNEST(SPLIT(SUBSTR(offer_image_embedding, 2 , LENGTH(offer_image_embedding) - 2))) e) AS offer_image_embedding,
    FROM k
)

SELECT
    item_id, ARRAY_CONCAT(embedding_offer_name, offer_description_embedding, offer_image_embedding) as embedding
FROM z