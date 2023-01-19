SELECT 
    diversification_raw.user_id
    , booking.offer_id
    , diversification_raw.booking_id
    , diversification_raw.booking_creation_date
    , booking.offer_category_id as category
    , booking.offer_subcategoryId as subcategory
    , offer.type
    , booking.venue_id as venue
    , booking.venue_name
    , user.user_region_name
    , booking.user_department_code
    , booking.user_activity
    , user.user_civility
    , booking.booking_amount
    , user.user_deposit_creation_date
    , COALESCE(
        IF(booking.physical_goods = True, 'physical', null),
        IF(booking.digital_goods = True, 'digital', null),
        IF(booking.event = True, 'event', null)
    ) as format
    , rayons.macro_rayon
    , {% for feature in params.diversification_features %}
        {{feature}}_diversification
        {% if not loop.last -%} , {%- endif %}
    {% endfor %}
    , delta_diversification
FROM `{{ bigquery_analytics_dataset }}.diversification_raw_v2` as diversification_raw
LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_booking_data` as booking
ON booking.booking_id = diversification_raw.booking_id
LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_offer_data` as offer
ON booking.offer_id = offer.offer_id
LEFT JOIN `{{ bigquery_analytics_dataset }}.macro_rayons` as rayons
ON offer.rayon = rayons.rayon
LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_user_data` as user
ON diversification_raw.user_id = user.user_id