SELECT  
    event_date,
    event_name,
    event_timestamp,
    offer_id as offer_id,
    user_id as user_id,
    unique_session_id,
    origin
FROM {{ source("analytics","firebase_pro_events") }}
WHERE 
    event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL {{ var("clickhouse_day_params",4) }} DAY) and DATE("{{ ds() }}")
AND user_pseudo_id is NOT NULL 
-- AND event_name in ("ConsultOffer","ConsultVenue","HasAddedOfferToFavorites")

