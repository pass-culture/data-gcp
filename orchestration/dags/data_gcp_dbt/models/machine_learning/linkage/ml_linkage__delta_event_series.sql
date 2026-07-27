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
    ),

    -- applicative event series this delta does not remove: they will still be
    -- there when the backend ingests, so an "add" on them is a no-op
    already_ingested_event_series as (
        select applicative_database_event_series.event_series_id
        from
            {{ source("raw", "applicative_database_event_series") }}
            as applicative_database_event_series
        where
            not exists (
                select 1 as found
                from casted_delta_event_series as delta
                where
                    delta.action in ('remove', 'update')
                    and delta.event_series_id
                    = applicative_database_event_series.event_series_id
            )
    ),

    added_delta_event_series as (
        select
            event_series_id,
            event_series_description,
            event_series_image_url,
            action,
            comment,
            event_name as event_series_name,
            regexp_extract(
                event_series_image_url, r'/mediations/([^/]+)'
            ) as event_series_mediation_uuid
        from casted_delta_event_series
        where
            action = 'add'
            -- these models are rebuilt daily while ingestion runs weekly: do not
            -- re-send an event series already ingested in the applicative database
            and not exists (
                select 1 as found
                from already_ingested_event_series as ingested
                where
                    ingested.event_series_id = casted_delta_event_series.event_series_id
            )
    ),

    removed_or_updated_delta_event_series as (
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
        inner join
            {{ source("raw", "applicative_database_event_series") }}
            as applicative_database_event_series
            on casted_delta_event_series.event_series_id
            = applicative_database_event_series.event_series_id
        where casted_delta_event_series.action in ('update', 'remove')
    )

select
    added_delta_event_series.event_series_id,
    added_delta_event_series.event_series_description,
    added_delta_event_series.event_series_image_url,
    added_delta_event_series.action,
    added_delta_event_series.comment,
    added_delta_event_series.event_series_name,
    added_delta_event_series.event_series_mediation_uuid
from added_delta_event_series
union all
select
    removed_or_updated_delta_event_series.event_series_id,
    removed_or_updated_delta_event_series.event_series_description,
    removed_or_updated_delta_event_series.event_series_image_url,
    removed_or_updated_delta_event_series.action,
    removed_or_updated_delta_event_series.comment,
    removed_or_updated_delta_event_series.event_series_name,
    removed_or_updated_delta_event_series.event_series_mediation_uuid
from removed_or_updated_delta_event_series
