select
    go.item_id,
    go.offer_name,
    go.offer_description,
    sum(case when ne.event_name = 'ConsultOffer' then 1 else 0 end) as item_vues
from `{{ bigquery_analytics_dataset }}.native_event` as ne
join `{{ bigquery_analytics_dataset }}.global_offer` as go on ne.offer_id = go.offer_id
where ne.event_date >= '2025-07-01' and ne.offer_id is not null

group by go.item_id, go.offer_name, go.offer_description
order by item_vues desc
limit 50
;
