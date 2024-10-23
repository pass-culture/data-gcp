with
    ranked_offers as (
        select unique_session_id, event_timestamp, offerer_id
        from {{ ref("int_firebase__pro_event") }}
        where offerer_id is not null
        qualify
            row_number() over (
                partition by unique_session_id order by event_timestamp desc
            )
            = 1
    ),

    ranked_venues as (
        select unique_session_id, event_timestamp, venue_id
        from {{ ref("int_firebase__pro_event") }}
        where venue_id is not null
        qualify
            row_number() over (
                partition by unique_session_id order by event_timestamp desc
            )
            = 1
    )

select distinct e.unique_session_id, e.user_id, ro.offerer_id, rv.venue_id
from {{ ref("int_firebase__pro_event") }} as e
left join ranked_offers as ro on e.unique_session_id = ro.unique_session_id
left join ranked_venues as rv on e.unique_session_id = rv.unique_session_id
