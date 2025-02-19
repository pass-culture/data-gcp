{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "event_date", "data_type": "date"},
        )
    )
}}

select
    event_date,
    e.offer_id,
    int_applicative__offer_item_id.item_id,
    origin,
    count(*) as nb_daily_consult
from {{ ref("int_firebase__native_event") }} e
left join
    {{ ref("int_applicative__offer_item_id") }} int_applicative__offer_item_id
    on int_applicative__offer_item_id.offer_id = e.offer_id
where
    event_name = 'ConsultOffer'
    {% if is_incremental() %}  -- recalculate latest day's DATA + previous
        and date(event_date) >= date_sub(date(_dbt_max_partition), interval 1 day)
    {% endif %}
group by 1, 2, 3, 4
