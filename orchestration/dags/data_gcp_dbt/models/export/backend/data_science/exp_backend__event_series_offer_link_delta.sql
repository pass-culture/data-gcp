-- depends_on: {{ ref('ml_linkage__future_event_series') }}
-- depends_on: {{ ref('ml_linkage__future_event_series_offer_link') }}
select event_series_id, offer_id, action, comment
from {{ ref("ml_linkage__delta_event_series_offer_link") }}
