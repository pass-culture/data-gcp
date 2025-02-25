SELECT
    new_item_id AS item_id,
    item_id_candidate AS offer_id
FROM
    `{{ bigquery_sandbox_dataset }}.linked_offer`
group by 1,2
UNION ALL

SELECT
    item_id_synchro AS item_id,
    item_id_candidate AS offer_id
FROM
    `{{ bigquery_sandbox_dataset }}.linked_product`
group by 1,2
