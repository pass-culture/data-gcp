select
    ado.offer_id,
    oii.item_id,
    ado.offer_subcategoryid,
    ado.offer_name,
    ado.offer_description,
    oed.performer
from `{{ bigquery_raw_dataset }}`.applicative_database_offer ado
left join
    `{{ bigquery_int_applicative_dataset }}`.offer_item_id oii
    on oii.offer_id = ado.offer_id
left join
    `{{ bigquery_int_applicative_dataset }}`.extract_offer oed
    on oed.offer_id = ado.offer_id
left join
    `{{ bigquery_analytics_dataset }}`.linked_offers ol
    on oii.item_id = ol.item_linked_id
where ol.item_linked_id is null and ado.offer_subcategoryid != 'LIVRE_PAPIER'
qualify row_number() over (partition by item_id) = 1
order by ado.offer_creation_date desc  -- to switch ASC
limit {{ params.batch_size }}
