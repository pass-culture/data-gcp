select event_id as event_series_id, offer_id, action, comment
from {{ source("ml_preproc", "delta_event_series_offer_link") }}
