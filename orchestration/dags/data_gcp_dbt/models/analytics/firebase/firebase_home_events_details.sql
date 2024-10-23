{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={
                "field": "event_date",
                "data_type": "date",
                "granularity": "day",
            },
            on_schema_change="sync_all_columns",
        )
    )
}}

with
    firebase_home_events as (
        select
            event_date,
            event_timestamp,
            session_id,
            user_id,
            user_pseudo_id,
            call_id,
            platform,
            event_name,
            event_type,
            offer_id,
            booking_id,
            module_name,
            content_type,
            module_id,
            home_id,
            module_index
        from {{ ref("firebase_home_events") }}
        where date(event_date) >= date_sub(date('{{ ds() }}'), interval 12 month)

    ),
    contentful_tags as (
        select
            entry_id,
            array_to_string(
                array_agg(coalesce(playlist_type, 'temporaire')), " / "
            ) as playlist_type
        from {{ ref("int_contentful__tag") }}
        group by entry_id
    ),
    diversification_booking as (
        -- in case we have more than one reservation for the same offer in the same
        -- day, take average (this should not happen).
        select
            date(booking_creation_date) as date,
            user_id,
            offer_id,
            avg(delta_diversification) as delta_diversification
        from {{ ref("diversification_booking") }}
        group by 1, 2, 3
    )

select
    date(e.event_date) as event_date,
    e.event_timestamp,
    e.session_id,
    e.user_id,
    e.user_pseudo_id,
    e.call_id,
    e.platform,
    e.event_name,
    e.event_type,
    e.offer_id,
    e.booking_id,
    e.module_name,
    e.content_type,
    e.module_id,
    e.home_id,
    e.module_index,
    coalesce(contentful_tags.playlist_type, 'temporaire') as playlist_type,
    ebd.booking_is_cancelled,
    case
        when event_type = "booking" then db.delta_diversification else null
    end as delta_diversification,
    if(
        event_type = "booking" and db.delta_diversification is not null, 1, 0
    ) as effective_booking,
    eud.user_department_code,
    eud.user_region_name,
    eud.current_deposit_type,
    eud.user_age,
    case
        when eud.current_deposit_type = 'GRANT_18'
        then 'Bénéficiaire 18-20 ans'
        when eud.current_deposit_type = 'GRANT_15_17'
        then 'Bénéficiaire 15-17 ans'
        when e.user_id is not null
        then 'Grand Public'
        else 'Non connecté'
    end as user_role,
    ee.title as home_name

from firebase_home_events e
left join contentful_tags contentful_tags on contentful_tags.entry_id = e.module_id
left join {{ ref("mrt_global__user") }} eud on e.user_id = eud.user_id
left join {{ ref("mrt_global__booking") }} ebd on e.booking_id = ebd.booking_id
left join {{ ref("int_contentful__entry") }} ee on e.home_id = ee.id
left join
    diversification_booking db
    on db.user_id = e.user_id
    and db.offer_id = e.offer_id
    and db.date = e.event_date
