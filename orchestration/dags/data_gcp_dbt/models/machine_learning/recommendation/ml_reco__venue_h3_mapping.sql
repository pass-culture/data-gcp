{{ config(materialized="table") }}

select distinct
    venue_id,
    venue_latitude as latitude,
    venue_longitude as longitude,
    string(
        bigfunctions.europe_west1.h3(
            "latLngToCell", to_json([venue_latitude, venue_longitude, 5])
        )
    ) as h3_res5
from {{ ref("ml_reco__recommendable_offer") }}
where
    venue_id is not null and venue_latitude is not null and venue_longitude is not null
