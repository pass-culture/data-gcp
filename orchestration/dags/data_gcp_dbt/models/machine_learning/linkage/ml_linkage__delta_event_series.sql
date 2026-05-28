select
    event_id as event_series_id,
    event_name as event_series_name,
    event_description as event_series_description,
    event_image_url as event_series_image_url,
    action,
    comment
from {{ source("ml_preproc", "delta_event_series") }}
