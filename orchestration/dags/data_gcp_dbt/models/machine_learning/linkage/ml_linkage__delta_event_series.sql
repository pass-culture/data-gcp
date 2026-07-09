with
    casted_delta_event_series as (
        select
            cast(event_id as string) as event_series_id,
            cast(event_description as string) as event_series_description,
            cast(event_image_url as string) as event_series_image_url,
            cast(action as string) as action,  -- noqa: disable=RF04
            cast(comment as string) as comment,
            cast(event_name as string) as event_name
        from {{ source("ml_preproc", "delta_event_series") }}
    )

select
    casted_delta_event_series.event_series_id,
    casted_delta_event_series.event_series_description,
    casted_delta_event_series.event_series_image_url,
    casted_delta_event_series.action,
    casted_delta_event_series.comment,
    case
        when casted_delta_event_series.action = 'remove'
        then applicative_database_event_series.event_series_name
        else casted_delta_event_series.event_name
    end as event_series_name,
    regexp_extract(
        casted_delta_event_series.event_series_image_url, r'/mediations/([^/]+)'
    ) as event_series_mediation_uuid
from casted_delta_event_series
left join
    {{ source("raw", "applicative_database_event_series") }}
    as applicative_database_event_series
    on casted_delta_event_series.event_series_id
    = applicative_database_event_series.event_series_id
