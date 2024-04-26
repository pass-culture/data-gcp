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
WHERE ado.offer_subcategoryId != 'LIVRE_PAPIER'
AND cast(ado.offer_id as STRING) not in (SELECT cast(offer_id as STRING) from `{{ bigquery_analytics_dataset }}`.offers_already_linked)
QUALIFY ROW_NUMBER() OVER (PARTITION BY item_id) = 1
order by ado.offer_creation_date ASC
LIMIT {{ params.batch_size }}