{% set shrinkage_threshold = 0.3 %}

with

    event_series_delta as (
        select
            event_series_id,
            event_series_name,
            event_series_description,
            event_series_image_url,
            event_series_mediation_uuid,
            action
        from {{ ref("exp_backend__event_series_delta") }}
    ),

    event_series_offer_link_delta as (
        select event_series_id, cast(offer_id as string) as offer_id, action
        from {{ ref("exp_backend__event_series_offer_link_delta") }}
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

    future_event_series_offer_link as (
        select base.event_series_id, cast(base.offer_id as string) as offer_id
        from {{ source("raw", "applicative_database_event_series_offer_link") }} as base
        where
            not exists (
                select 1 as found
                from event_series_offer_link_delta as delta
                where
                    delta.action in ("remove", "update")
                    and delta.event_series_id = base.event_series_id
                    and delta.offer_id = cast(base.offer_id as string)
            )
        union all
        select event_series_id, offer_id
        from event_series_offer_link_delta
        where action in ("add", "update")
    ),

    -- ----------------- DELTA CHECKS -------------------
    check_event_series_id_in_delta as (
        select
            countif(
                event_series_id is null or event_series_id = ""
            ) as count_null_or_empty_event_series_id_in_event_series_delta
        from event_series_delta
    ),

    check_event_series_name_in_delta as (
        select
            countif(
                event_series_name is null or event_series_name = ""
            ) as count_null_or_empty_event_series_name_in_event_series_delta
        from event_series_delta
    ),

    check_invalid_actions_in_event_series_delta as (
        select
            countif(
                action is null or action not in ("add", "update", "remove")
            ) as count_invalid_actions_in_event_series_delta
        from event_series_delta
    ),

    check_invalid_actions_in_event_series_offer_link_delta as (
        select
            countif(
                action is null or action not in ("add", "update", "remove")
            ) as count_invalid_actions_in_event_series_offer_link_delta
        from event_series_offer_link_delta
    ),

    check_missing_mediation_uuid_in_event_series_delta as (
        select
            countif(
                event_series_image_url is not null
                and event_series_image_url != ""
                and (
                    event_series_mediation_uuid is null
                    or event_series_mediation_uuid = ""
                )
            ) as count_missing_mediation_uuid_in_event_series_delta
        from event_series_delta
        where action in ("add", "update")
    ),

    -- ----------------- FUTURE CHECKS -------------------
    check_event_series_id_duplicates_in_future_event_series as (
        select
            count(*) - count(
                distinct event_series_id
            ) as count_duplicate_event_series_id_in_future_event_series
        from future_event_series
    ),

    check_event_series_name_in_future_event_series as (
        select
            countif(
                event_series_name is null or event_series_name = ""
            ) as count_null_or_empty_event_series_name_in_future_event_series
        from future_event_series
    ),

    check_duplicate_links_in_future_event_series_offer_link as (
        select
            count(*) - count(
                distinct to_json_string(struct(offer_id, event_series_id))
            ) as count_duplicate_links_in_future_event_series_offer_link
        from future_event_series_offer_link
    ),

    check_orphan_event_series_in_future_event_series_offer_link as (
        select
            countif(
                not exists (
                    select 1 as found
                    from future_event_series
                    where
                        future_event_series.event_series_id
                        = future_event_series_offer_link.event_series_id
                )
            ) as count_orphan_event_series_in_future_event_series_offer_link
        from future_event_series_offer_link
    ),

    check_event_series_id_null_or_empty_in_future_event_series as (
        select
            countif(
                event_series_id is null or event_series_id = ""
            ) as count_null_or_empty_event_series_id_in_future_event_series
        from future_event_series
    ),

    check_null_or_empty_keys_in_future_event_series_offer_link as (
        select
            countif(
                event_series_id is null
                or event_series_id = ""
                or offer_id is null
                or offer_id = ""
            ) as count_null_or_empty_keys_in_future_event_series_offer_link
        from future_event_series_offer_link
    ),

    check_offers_linked_to_multiple_event_series_in_future as (
        select
            count(distinct to_json_string(struct(event_series_id, offer_id)))
            - count(distinct offer_id) as count_multi_event_series_offers_in_future
        from future_event_series_offer_link
    ),

    -- -------------------- VOLUME CHECKS ----------------------
    check_future_event_series_shrinkage as (
        select
            (
                (select count(*) as row_count from future_event_series) < (
                    select count(*) as row_count
                    from {{ source("raw", "applicative_database_event_series") }}
                )
                * {{ shrinkage_threshold }}
            ) as is_future_event_series_below_shrinkage_threshold
    ),

    check_future_event_series_offer_link_shrinkage as (
        select
            (
                (select count(*) as row_count from future_event_series_offer_link) < (
                    select count(*) as row_count
                    from
                        {{
                            source(
                                "raw", "applicative_database_event_series_offer_link"
                            )
                        }}
                )
                * {{ shrinkage_threshold }}
            ) as is_future_event_series_offer_link_below_shrinkage_threshold
    ),

    aggregated_checks as (
        select
            -- - DELTA CHECKS ---
            check_event_series_id_in_delta.count_null_or_empty_event_series_id_in_event_series_delta,
            check_event_series_name_in_delta.count_null_or_empty_event_series_name_in_event_series_delta,
            check_invalid_actions_in_event_series_delta.count_invalid_actions_in_event_series_delta,
            check_invalid_actions_in_event_series_offer_link_delta.count_invalid_actions_in_event_series_offer_link_delta,
            check_missing_mediation_uuid_in_event_series_delta.count_missing_mediation_uuid_in_event_series_delta,
            -- - FUTURE CHECKS ---
            check_event_series_id_duplicates_in_future_event_series.count_duplicate_event_series_id_in_future_event_series,
            check_event_series_name_in_future_event_series.count_null_or_empty_event_series_name_in_future_event_series,
            check_duplicate_links_in_future_event_series_offer_link.count_duplicate_links_in_future_event_series_offer_link,
            check_orphan_event_series_in_future_event_series_offer_link.count_orphan_event_series_in_future_event_series_offer_link,
            check_event_series_id_null_or_empty_in_future_event_series.count_null_or_empty_event_series_id_in_future_event_series,
            check_null_or_empty_keys_in_future_event_series_offer_link.count_null_or_empty_keys_in_future_event_series_offer_link,
            check_offers_linked_to_multiple_event_series_in_future.count_multi_event_series_offers_in_future,
            -- - VOLUME CHECKS ---
            check_future_event_series_shrinkage.is_future_event_series_below_shrinkage_threshold,
            check_future_event_series_offer_link_shrinkage.is_future_event_series_offer_link_below_shrinkage_threshold

        from check_event_series_id_duplicates_in_future_event_series
        cross join check_event_series_name_in_future_event_series
        cross join check_duplicate_links_in_future_event_series_offer_link
        cross join check_orphan_event_series_in_future_event_series_offer_link
        cross join check_event_series_id_null_or_empty_in_future_event_series
        cross join check_null_or_empty_keys_in_future_event_series_offer_link
        cross join check_invalid_actions_in_event_series_delta
        cross join check_invalid_actions_in_event_series_offer_link_delta
        cross join check_offers_linked_to_multiple_event_series_in_future
        cross join check_missing_mediation_uuid_in_event_series_delta
        cross join check_future_event_series_shrinkage
        cross join check_future_event_series_offer_link_shrinkage
        cross join check_event_series_id_in_delta
        cross join check_event_series_name_in_delta
    )

select
    *,
    (
        (
            count_duplicate_event_series_id_in_future_event_series
            + count_null_or_empty_event_series_name_in_future_event_series
            + count_duplicate_links_in_future_event_series_offer_link
            + count_orphan_event_series_in_future_event_series_offer_link
            + count_null_or_empty_event_series_id_in_future_event_series
            + count_null_or_empty_keys_in_future_event_series_offer_link
            + count_invalid_actions_in_event_series_delta
            + count_invalid_actions_in_event_series_offer_link_delta
            + count_multi_event_series_offers_in_future
            + count_missing_mediation_uuid_in_event_series_delta
            + count_null_or_empty_event_series_id_in_event_series_delta
            + count_null_or_empty_event_series_name_in_event_series_delta
        )
        = 0
        and (
            not is_future_event_series_below_shrinkage_threshold
            and not is_future_event_series_offer_link_below_shrinkage_threshold
        )
    ) as ready_for_ingestion
from aggregated_checks
