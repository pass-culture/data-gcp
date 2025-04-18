with
    all_links as (
        select new_item_id as item_id, item_id_candidate as raw_offer_id_int
        from `{{ bigquery_sandbox_dataset }}.linked_offer`

        union all

        select item_id_synchro as item_id, item_id_candidate as raw_offer_id_int
        from `{{ bigquery_sandbox_dataset }}.linked_product`
    )

select item_id, replace(string(raw_offer_id_int), 'offer-', '') as offer_id
from all_links
group by item_id, offer_id_str, offer_id
;
