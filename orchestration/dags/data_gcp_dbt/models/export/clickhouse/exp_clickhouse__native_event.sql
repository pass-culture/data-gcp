SELECT  
    event_date as partition_date,
    event_name,
    event_timestamp,
    offer_id as offer_id,
    user_id as user_id,
    unique_session_id,
    venue_id,
    origin,
    is_consult_offer,
    is_consult_venue,
    is_add_to_favorites
FROM {{ ref("int_firebase__native_event") }}
WHERE event_name in ("ConsultOffer","ConsultVenue","HasAddedOfferToFavorites")