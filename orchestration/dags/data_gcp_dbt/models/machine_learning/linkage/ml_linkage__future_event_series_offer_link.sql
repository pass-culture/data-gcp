-- depends_on: {{ ref('ml_linkage__future_event_series') }}
with
    event_series_offer_link_delta as (
        select event_series_id, cast(offer_id as string) as offer_id, action
        from {{ ref("ml_linkage__delta_event_series_offer_link") }}
    ),

    future_event_series_offer_link as (
        select base.event_series_id, cast(base.offer_id as string) as offer_id
        from {{ source("raw", "applicative_database_event_series_offer_link") }} as base
        where
            not exists (
                select 1 as found
                from event_series_offer_link_delta as delta
                where
                    delta.action in ("remove", "update")
                    and delta.event_series_id = base.event_series_id
                    and delta.offer_id = cast(base.offer_id as string)
            )
        union all
        select event_series_id, offer_id
        from event_series_offer_link_delta
        where action in ("add", "update")
    )

select event_series_id, offer_id
from future_event_series_offer_link
