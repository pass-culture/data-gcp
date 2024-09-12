{% set diversification_features = ["category", "sub_category", "format", "venue_id", "extra_category", "venue_type_label"] %}

select
    diversification_raw.user_id,
    booking.offer_id,
    diversification_raw.booking_id,
    diversification_raw.booking_creation_date,
    offer_category_id as category,
    offer_subcategory_id as subcategory,
    offer_type_label,
    booking.venue_id as venue,
    booking.venue_name,
    user.user_region_name,
    booking.user_department_code,
    booking.user_activity,
    user.user_civility,
    booking.booking_intermediary_amount as booking_amount,
    user.first_deposit_creation_date,
    COALESCE(
        IF(booking.physical_goods = True, 'physical', Null),
        IF(booking.digital_goods = True, 'digital', Null),
        IF(booking.event = True, 'event', Null)
    ) as format
    , {% for feature in diversification_features %}
        {{ feature }}_diversification
        {% if not loop.last -%} , {%- endif %}
    {% endfor %}
    , delta_diversification
from {{ ref('diversification_raw') }} as diversification_raw
    left join {{ ref('mrt_global__booking') }} as booking
        on booking.booking_id = diversification_raw.booking_id
    left join {{ ref('int_applicative__offer_metadata') }} as offer_metadata
        on booking.offer_id = offer_metadata.offer_id
    left join {{ ref('mrt_global__user') }} as user
        on diversification_raw.user_id = user.user_id
