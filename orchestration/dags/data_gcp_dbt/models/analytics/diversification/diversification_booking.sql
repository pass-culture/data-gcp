{% set diversification_features = ["category", "sub_category", "format", "venue_id", "extra_category", "venue_type_label"] %}

SELECT 
    diversification_raw.user_id
    , booking.offer_id
    , diversification_raw.booking_id
    , diversification_raw.booking_creation_date
    , category_id as category
    , subcategory_id as subcategory
    , offer_type_label
    , booking.venue_id as venue
    , booking.venue_name
    , user.user_region_name
    , booking.user_department_code
    , booking.user_activity
    , user.user_civility
    , booking.booking_intermediary_amount as booking_amount
    , user.user_deposit_creation_date
    , COALESCE(
        IF(booking.physical_goods = True, 'physical', null),
        IF(booking.digital_goods = True, 'digital', null),
        IF(booking.event = True, 'event', null)
    ) as format
    , {% for feature in diversification_features %}
        {{ feature }}_diversification
        {% if not loop.last -%} , {%- endif %}
    {% endfor %}
    , delta_diversification
FROM {{ ref('diversification_raw') }} as diversification_raw
LEFT JOIN {{ ref('mrt_global__booking') }} as booking
ON booking.booking_id = diversification_raw.booking_id
LEFT JOIN {{ ref('offer_metadata') }} as offer_metadata
ON booking.offer_id = offer_metadata.offer_id
LEFT JOIN {{ ref('enriched_user_data') }} as user
ON diversification_raw.user_id = user.user_id