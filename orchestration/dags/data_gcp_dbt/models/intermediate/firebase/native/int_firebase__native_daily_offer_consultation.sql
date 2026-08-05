{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "event_date", "data_type": "date"},
        )
    )
}}

select
    e.event_date,
    e.offer_id,
    int_applicative__offer_item_id.item_id,
    e.origin,
    count(*) as nb_daily_consult
from {{ ref("int_firebase__native_event") }} as e
left join
    {{ ref("int_applicative__offer_item_id") }} as int_applicative__offer_item_id
    on e.offer_id = int_applicative__offer_item_id.offer_id
where
    e.event_name = 'ConsultOffer'
    {% if is_incremental() %}  -- recalculate latest day's DATA + previous
        and date(e.event_date) >= date_sub(date(_dbt_max_partition), interval 1 day)
    {% endif %}
group by 1, 2, 3, 4
