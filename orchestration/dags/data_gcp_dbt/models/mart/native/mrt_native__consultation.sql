{{
    config(
        **custom_incremental_config(
        incremental_strategy = 'insert_overwrite',
        partition_by = {'field': 'consultation_date', 'data_type': 'date'}
    )
) }}

with discoveries_by_consultation as (
SELECT 
    consultation_id,
    MAX(CASE WHEN type = 'item' and consulted_entity is not null then 1 else 0 END) AS item_score,
    MAX(CASE WHEN type = 'offer_subcategory' and consulted_entity is not null then 1 else 0 END) AS subcategory_score,
    MAX(CASE WHEN type = 'offer_category' and consulted_entity is not null then 1 else 0 END) AS category_score,
from {{ ref('int_metric__discovery_score')}}
group by consultation_id
)

SELECT 
    consult.consultation_id,
    consult.consultation_date,
    consult.origin,
    consult.offer_id,
    consult.user_id,
    consult.unique_session_id,
    dc.item_discovery_score,
    dc.subcategory_discovery_score,
    dc.category_discovery_score,
    dc.item_score + dc.subcategory_score + dc.category_score as discovery_score,
    case when category_score > 0 then true else false end as is_category_discovered,
    case when subcategory_score > 0 then true else false end as is_subcategory_discovered,
    offer.item_id,
    offer.offer_subcategory_id,
    offer.offer_category_id,
    offer.offer_name,
    offer.venue_id,
    offer.venue_name,
    offer.venue_type_label,
    offer.partner_id,
    offer.offerer_id,
    user.user_region_name,
    user.user_department_code,
    user.user_activity,
    user.user_is_priority_public,
    user.user_is_unemployed,
    user.user_is_in_qpv,
    user.user_macro_density_label,
    consult.traffic_medium,
    consult.traffic_campaign,
    consult.module_id
FROM {{ ref('int_firebase__native_consultation')}} AS consult
left join discoveries_by_consultation as dc on dc.consultation_id = consult.consultation_id
left join {{ ref('int_global__offer')}} as offer on consult.offer_id = offer.offer_id
left join {{ ref('int_global__user')}} as user on consult.user_id = user.user_id
{% if is_incremental() %}
where consultation_date >= date_sub('{{ ds() }}', INTERVAL 3 day)
{% endif %}
