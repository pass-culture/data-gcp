with
    casted_delta_event_series_offer_link as (
        select
            cast(event_id as string) as event_series_id,
            cast(offer_id as string) as offer_id,
            cast(action as string) as action,  -- noqa: disable=RF04
            cast(comment as string) as comment
        from {{ source("ml_preproc", "delta_event_series_offer_link") }}
    ),

    added_delta_event_series_offer_link as (
        select event_series_id, offer_id, action, comment
        from casted_delta_event_series_offer_link
        where action = 'add'
    ),

    removed_or_updated_delta_event_series_offer_link as (
        select
            casted_delta_event_series_offer_link.event_series_id,
            casted_delta_event_series_offer_link.offer_id,
            casted_delta_event_series_offer_link.action,
            casted_delta_event_series_offer_link.comment
        from casted_delta_event_series_offer_link
        inner join
            {{ source("raw", "applicative_database_event_series_offer_link") }}
            as applicative_database_event_series_offer_link
            on casted_delta_event_series_offer_link.event_series_id
            = applicative_database_event_series_offer_link.event_series_id
            and casted_delta_event_series_offer_link.offer_id
            = cast(applicative_database_event_series_offer_link.offer_id as string)
        where casted_delta_event_series_offer_link.action in ('update', 'remove')
    )

select *
from added_delta_event_series_offer_link
union all
select *
from removed_or_updated_delta_event_series_offer_link
