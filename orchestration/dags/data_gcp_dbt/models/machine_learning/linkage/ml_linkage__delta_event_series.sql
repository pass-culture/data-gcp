select
    delta_event_series.event_id as event_series_id,
    delta_event_series.event_description as event_series_description,
    delta_event_series.event_image_url as event_series_image_url,
    delta_event_series.action,
    delta_event_series.comment,
    case
        when delta_event_series.action = 'remove'
        then applicative_database_event_series.event_series_name
        else delta_event_series.event_name
    end as event_series_name,
    regexp_extract(
        delta_event_series.event_image_url, r'/mediations/([^/]+)'
    ) as event_series_mediation_uuid
from {{ source("ml_preproc", "delta_event_series") }} as delta_event_series
left join
    {{ source("raw", "applicative_database_event_series") }}
    as applicative_database_event_series
    on delta_event_series.event_id = applicative_database_event_series.event_series_id
