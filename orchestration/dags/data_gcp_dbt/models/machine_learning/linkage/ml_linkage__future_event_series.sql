with

    event_series_delta as (
        select
            event_series_id,
            event_series_name,
            event_series_description,
            event_series_mediation_uuid,
            action
        from {{ ref("ml_linkage__delta_event_series") }}
    ),

    future_event_series as (
        select
            base.event_series_id,
            base.event_series_name,
            base.event_series_description,
            base.event_series_mediation_uuid
        from {{ source("raw", "applicative_database_event_series") }} as base
        where
            not exists (
                select 1 as found
                from event_series_delta as delta
                where
                    delta.action in ("remove", "update")
                    and delta.event_series_id = base.event_series_id
            )
        union all
        select
            event_series_id,
            event_series_name,
            cast(event_series_description as string) as event_series_description,
            event_series_mediation_uuid
        from event_series_delta
        where action in ("add", "update")
    ),

select
    event_series_id,
    event_series_name,
    event_series_description,
    event_series_mediation_uuid
from future_event_series
