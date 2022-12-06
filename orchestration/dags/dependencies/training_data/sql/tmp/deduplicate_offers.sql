WITH event_offers_table AS
(
  SELECT * except (item_id) from `{{ bigquery_analytics_dataset }}`.`enriched_offer_data` where offer_subcategoryId IN {{ params.event_subcategories }}
),
linked_offers_table AS
(
  SELECT offer_name as deduplicated_offer_name, MIN(item_id) as item_id FROM `{{ bigquery_analytics_dataset }}`.`enriched_offer_data` where offer_subcategoryId IN {{ params.event_subcategories }} group by offer_name
),
deduplicated_event_offers_table AS
(
  SELECT * FROM event_offers_table LEFT JOIN linked_offers_table
  ON
    event_offers_table.offer_name=linked_offers_table.deduplicated_offer_name
),
not_event_offers_table AS
(
  SELECT * except (item_id), item_id from `{{ bigquery_analytics_dataset }}`.`enriched_offer_data`
  where offer_subcategoryId NOT IN {{ params.event_subcategories }}
)
SELECT * except (deduplicated_offer_name) FROM deduplicated_event_offers_table
UNION ALL
SELECT * from not_event_offers_table
order by offer_name