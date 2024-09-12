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
    array_agg(distinct int_applicative__offer_item_id.item_id) as consulted_items,
    array_agg(distinct firebase.origin) as consulted_origins
from {{ ref('user_beneficiary') }} user
    join {{ ref('int_firebase__native_event') }} firebase
        on user.user_id = firebase.user_id
            and event_name = 'ConsultOffer'
            {% if is_incremental() %}
                -- recalculate latest day's data
                and date(event_date) = date_sub('{{ ds() }}', interval 1 day)
            {% endif %}
    join {{ ref('int_applicative__offer_item_id') }} int_applicative__offer_item_id -- retire les offres non reliées à un item_id
        on firebase.offer_id = int_applicative__offer_item_id.offer_id
group by 1, 2
