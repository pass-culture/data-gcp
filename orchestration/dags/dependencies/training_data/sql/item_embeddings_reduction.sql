SELECT 
    * 
FROM `{{ bigquery_clean_dataset }}.item_embeddings`
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY item_id
    ORDER BY
    extraction_date DESC
) = 1