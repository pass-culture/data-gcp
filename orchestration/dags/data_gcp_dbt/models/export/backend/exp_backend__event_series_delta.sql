select
    event_series_id,
    event_series_name,
    event_series_description,
    event_series_image_url,
    action,
    comment
from {{ ref("ml_linkage__delta_event_series") }}
