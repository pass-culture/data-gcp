{% set diversification_features = [
    "category",
    "sub_category",
    "format",
    "venue_id",
    "extra_category",
    "venue_type_label",
] %}

select
    diversification_raw.user_id,
    booking.offer_id,
    diversification_raw.booking_id,
    diversification_raw.booking_creation_date,
    booking.offer_category_id as category,
    booking.offer_subcategory_id as subcategory,
    booking.offer_type_label,
    booking.venue_id as venue,
    booking.venue_name,
    booking.user_region_name,
    booking.user_department_code,
    booking.user_activity,
    booking.user_civility,
    booking.booking_intermediary_amount as booking_amount,
    booking.first_deposit_creation_date,
    coalesce(
        if(booking.physical_goods = true, 'physical', null),
        if(booking.digital_goods = true, 'digital', null),
        if(booking.event = true, 'event', null)
    ) as format,
    {% for feature in diversification_features %}
        {{ feature }}_diversification {% if not loop.last -%}, {%- endif %}
    {% endfor %},
    delta_diversification
from {{ ref("diversification_raw") }} as diversification_raw
left join
    {{ ref("mrt_global__booking") }} as booking
    on booking.booking_id = diversification_raw.booking_id
