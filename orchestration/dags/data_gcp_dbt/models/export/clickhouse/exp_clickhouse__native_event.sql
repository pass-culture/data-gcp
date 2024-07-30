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
AND 
    -- ensure the params are consistent with the event
    CASE
        WHEN is_consult_offer = 1 THEN offer_id is not NULL
        WHEN is_consult_venue = 1 THEN venue_id is not NULL
        WHEN is_add_to_favorites = 1 THEN offer_id is not NULL
    END