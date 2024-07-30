{{
    config(
        **custom_incremental_config(
        incremental_strategy='insert_overwrite',
        partition_by={'field': 'event_date', 'data_type': 'date'},
    )
) }}

select
    event_date,
    e.offer_id,
    offer_item_ids.item_id,
    origin,
    COUNT(*) as nb_daily_consult
from
    {{ ref('int_firebase__native_event') }} e
    left join
        {{ ref('offer_item_ids') }} offer_item_ids
        on
            offer_item_ids.offer_id = e.offer_id
where
    event_name = 'ConsultOffer'
    {% if is_incremental() %} -- recalculate latest day's DATA + previous
        and
        DATE(event_date) >= DATE_SUB(DATE(_dbt_max_partition), interval 1 day)
{% endif %}
group by
    1,
    2,
    3,
    4
