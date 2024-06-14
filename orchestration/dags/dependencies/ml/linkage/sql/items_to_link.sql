SELECT
ado.offer_id,
oii.item_id,
ado.offer_subcategoryId,
ado.offer_name,
ado.offer_description,
oed.performer
FROM `{{ bigquery_analytics_dataset }}`.applicative_database_offer ado
LEFT JOIN `{{ bigquery_clean_dataset }}`.offer_item_ids oii on oii.offer_id = ado.offer_id 
LEFT JOIN `{{ bigquery_analytics_dataset }}`.offer_extracted_data oed on oed.offer_id = ado.offer_id 
LEFT JOIN `{{ bigquery_analytics_dataset }}`.linked_offers ol on oii.item_id=ol.item_linked_id
WHERE ol.item_linked_id is null
AND ado.offer_subcategoryId != 'LIVRE_PAPIER'
QUALIFY ROW_NUMBER() OVER (PARTITION BY item_id) = 1
ORDER BY ado.offer_creation_date DESC --to switch ASC
LIMIT {{ params.batch_size }}