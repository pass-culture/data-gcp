select
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
from {{ ref("int_firebase__native_event") }}
where
    event_name in ("ConsultOffer", "ConsultVenue", "HasAddedOfferToFavorites")
    and
    -- ensure the params are consistent with the event
    case
        when is_consult_offer = 1 then offer_id is not NULL
        when is_consult_venue = 1 then venue_id is not NULL
        when is_add_to_favorites = 1 then offer_id is not NULL
    end
