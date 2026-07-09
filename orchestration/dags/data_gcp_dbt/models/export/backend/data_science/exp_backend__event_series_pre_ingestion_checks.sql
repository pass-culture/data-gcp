with
    future_event_series as (
        select
            event_series_id,
            event_series_name,
            event_series_description,
            event_series_mediation_uuid
        from {{ source("raw", "applicative_database_event_series") }}
        where
            event_series_id not in (
                select distinct event_series_delta.event_series_id
                from {{ ref("exp_backend__event_series_delta") }} as event_series_delta
                where
                    event_series_delta.action = "remove"
                    or event_series_delta.action = "update"
            )
        union all
        select
            event_series_id,
            event_series_name,
            cast(event_series_description as string) as event_series_description,
            event_series_mediation_uuid
        from {{ ref("exp_backend__event_series_delta") }}
        where action = "add" or action = "update"
    ),

    future_event_series_offer_link as (
        select
            event_series_id,
            cast(offer_id as string) as offer_id,
            concat(event_series_id, offer_id) as concat_id
        from {{ source("raw", "applicative_database_event_series_offer_link") }}
        where
            concat(event_series_id, offer_id) not in (

                select
                    concat(
                        event_series_offer_link_delta.event_series_id,
                        event_series_offer_link_delta.offer_id
                    ) as concat_id
                from
                    {{ ref("exp_backend__event_series_offer_link_delta") }}
                    as event_series_offer_link_delta
                where
                    event_series_offer_link_delta.action = "remove"
                    or event_series_offer_link_delta.action = "update"
            )
        union all
        select event_series_id, offer_id, concat(event_series_id, offer_id) as concat_id
        from {{ ref("exp_backend__event_series_offer_link_delta") }}
        where action = "add" or action = "update"
    ),

    check_event_series_id_duplicates_in_future_event_series as (
        select
            count(*) - count(
                distinct event_series_id
            ) as count_event_series_id_duplicates_in_future_event_series
        from future_event_series
    ),

    check_event_series_name_in_future_event_series as (
        select
            countif(
                event_series_name is null or event_series_name = ""
            ) as count_event_series_name_null_or_empty_in_future_event_series
        from future_event_series
    ),

    check_duplicate_links_in_future_event_series_offer_link as (
        select
            count(*) - count(
                distinct to_json_string(struct(offer_id, event_series_id))
            ) as count_duplicate_links_in_future_event_series_offer_link
        from future_event_series_offer_link
    ),

    aggregated_checks as (
        select
            check_event_series_id_duplicates_in_future_event_series.count_event_series_id_duplicates_in_future_event_series,
            check_event_series_name_in_future_event_series.count_event_series_name_null_or_empty_in_future_event_series,
            check_duplicate_links_in_future_event_series_offer_link.count_duplicate_links_in_future_event_series_offer_link

        from check_event_series_id_duplicates_in_future_event_series
        cross join check_event_series_name_in_future_event_series
        cross join check_duplicate_links_in_future_event_series_offer_link
    )

select
    *,
    (
        count_event_series_id_duplicates_in_future_event_series
        + count_event_series_name_null_or_empty_in_future_event_series
        + count_duplicate_links_in_future_event_series_offer_link
    )
    = 0 as ready_for_ingestion
from aggregated_checks
