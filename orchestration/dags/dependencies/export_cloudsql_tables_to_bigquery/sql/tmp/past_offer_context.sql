select
    id,
    call_id,
    context,
    context_extra_data,
    date,
    user_id,
    user_bookings_count,
    user_clicks_count,
    user_favorites_count,
    user_deposit_remaining_credit,
    user_iris_id,
    user_is_geolocated,
    user_extra_data,
    offer_user_distance,
    offer_is_geolocated,
    offer_id,
    offer_item_id,
    offer_booking_number,
    offer_stock_price,
    offer_creation_date,
    offer_stock_beginning_date,
    offer_category,
    offer_subcategory_id,
    offer_item_rank,
    offer_item_score,
    offer_order,
    offer_venue_id,
    offer_extra_data
from public.past_offer_context
