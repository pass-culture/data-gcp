{% set diversity_features = [
    "category",
    "sub_category",
    "format",
    "venue_id",
    "extra_category",
    "venue_type_label",
] %}

select
    diversity_raw.user_id,
    booking.offer_id,
    diversity_raw.booking_id,
    diversity_raw.booking_creation_date,
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
    {% for feature in diversity_features %}
        {{ feature }}_diversity {% if not loop.last -%}, {%- endif %}
    {% endfor %},
    diversity_raw.delta_diversity
from {{ ref("diversity_raw") }} as diversity_raw
left join
    {{ ref("mrt_global__booking") }} as booking
    on booking.booking_id = diversity_raw.booking_id
