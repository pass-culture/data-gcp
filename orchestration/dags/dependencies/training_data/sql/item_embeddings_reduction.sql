SELECT 
    ie.* 
FROM `{{ bigquery_clean_dataset }}.item_embeddings` ie
INNER JOIN `{{ bigquery_clean_dataset }}.offer_item_ids` oii on oii.item_id = ie.item_id

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ie.item_id
    ORDER BY
    ie.extraction_date DESC
) = 1