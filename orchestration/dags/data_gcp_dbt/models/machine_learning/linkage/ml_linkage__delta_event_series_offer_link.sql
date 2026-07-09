select
    cast(event_id as string) as event_series_id,
    cast(offer_id as string) as offer_id,
    cast(action as string) as action,  -- noqa: disable=RF04
    cast(comment as string) as comment
from {{ source("ml_preproc", "delta_event_series_offer_link") }}
