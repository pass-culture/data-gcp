select new_item_id as item_id, replace(item_id_candidate, 'offer-', '') as offer_id
from `{{ bigquery_sandbox_dataset }}.linked_offer`
group by 1, 2
union all
select item_id_synchro as item_id, replace(item_id_candidate, 'offer-', '') as offer_id
from `{{ bigquery_sandbox_dataset }}.linked_product`
group by 1, 2
