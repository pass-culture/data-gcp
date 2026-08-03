-- depends_on: {{ ref('ml_linkage__future_event_series') }}
-- depends_on: {{ ref('ml_linkage__future_event_series_offer_link') }}
select
    event_series_id,
    event_series_name,
    event_series_description,
    event_series_image_url,
    event_series_mediation_uuid,
    action,
    comment
from {{ ref("ml_linkage__delta_event_series") }}
