{{
    config(
        **custom_incremental_config(
        incremental_strategy = 'insert_overwrite',
        partition_by = {'field': 'consultation_date', 'data_type': 'date'}
    )
) }}

select
    user.user_id,
    event_date as consultation_date,
    array_agg(distinct offer_item_ids.item_id) as consulted_items,
    array_agg(distinct firebase.origin) as consulted_origins
from {{ ref('user_beneficiary') }} user
    join {{ ref('int_firebase__native_event') }} firebase
        on user.user_id = firebase.user_id
            and event_name = 'ConsultOffer'
            {% if is_incremental() %}
                -- recalculate latest day's data
                and date(event_date) = date_sub('{{ ds() }}', interval 1 day)
            {% endif %}
    join {{ ref('offer_item_ids') }} offer_item_ids -- retire les offres non reliées à un item_id
        on firebase.offer_id = offer_item_ids.offer_id
group by 1, 2
