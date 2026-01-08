{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={
                "field": "booking_date",
                "data_type": "date",
                "granularity": "day",
            },
            on_schema_change="append_new_columns",
            cluster_by="module_name_first_touch",
        )
    )
}}

with
    firebase_bookings as (
        select
            booking_date as event_date,
            session_id,
            unique_session_id,
            app_version,
            platform,
            user_location_type,
            booking_id,
            timestamp(booking_timestamp) as event_timestamp
        from {{ ref("firebase_bookings") }}

        {% if is_incremental() %}
            where
                date(
                    booking_date
                ) between date_sub(date('{{ ds() }}'), interval 3 day) and date(
                    '{{ ds() }}'
                )
        {% endif %}
    ),

    all_bookings_reconciled as (
        select
            booking.user_id,
            f_events.session_id as booking_session_id,
            f_events.unique_session_id as booking_unique_session_id,
            f_events.app_version as booking_app_version,
            booking.offer_id,
            booking.offer_category_id,
            booking.offer_subcategory_id,
            booking.deposit_id,
            booking.booking_status,
            booking.booking_is_cancelled,
            booking.booking_intermediary_amount,
            booking.item_id,
            booking.booking_id,
            f_events.platform,
            f_events.user_location_type,
            coalesce(f_events.event_date, date(booking_created_at)) as booking_date,
            coalesce(
                f_events.event_timestamp, timestamp(booking_created_at)
            ) as booking_timestamp
        from {{ ref("mrt_global__booking") }} as booking
        left join
            firebase_bookings as f_events on booking.booking_id = f_events.booking_id

        {% if is_incremental() %}
            where
                date(
                    booking_created_at
                ) between date_sub(date('{{ ds() }}'), interval 3 day) and date(
                    '{{ ds() }}'
                )
        {% endif %}
    ),

    firebase_consult as (
        select
            user_id,
            offer_id,
            item_id,
            event_date as consult_date,
            origin as consult_origin,
            reco_call_id,
            search_id,
            module_id,
            module_name,
            entry_id,
            timestamp(event_timestamp) as consult_timestamp
        from {{ ref("int_firebase__native_event") }}
        inner join
            {{ ref("int_applicative__offer_item_id") }}
            as int_applicative__offer_item_id using (offer_id)
        where
            event_name = 'ConsultOffer'
            and origin
            not in ('offer', 'endedbookings', 'bookingimpossible', 'bookings')
            {% if is_incremental() %}
                and date(
                    event_date
                ) between date_sub(date('{{ ds() }}'), interval 10 day) and date(
                    '{{ ds() }}'
                )  -- 3 + 7 days lag
            {% endif %}
    ),

    bookings_origin_first_touch as (
        select
            all_bookings_reconciled.user_id,
            booking_date,
            booking_timestamp,
            booking_session_id,
            booking_unique_session_id,
            reco_call_id,
            all_bookings_reconciled.offer_id,
            all_bookings_reconciled.item_id,
            booking_id,
            consult_date,
            consult_timestamp,
            consult_origin as consult_origin_first_touch,
            platform,
            search_id,
            all_bookings_reconciled.user_location_type,
            module_id as module_id_first_touch,
            module_name as module_name_first_touch,
            entry_id as home_id_first_touch
        from all_bookings_reconciled
        inner join
            firebase_consult
            on all_bookings_reconciled.user_id = firebase_consult.user_id
            and all_bookings_reconciled.item_id = firebase_consult.item_id
            and consult_date >= date_sub(booking_date, interval 7 day)  -- force 7 days lag max
            and consult_timestamp <= timestamp_add(booking_timestamp, interval 5 minute)
        qualify
            row_number() over (
                partition by
                    booking_id, firebase_consult.user_id, firebase_consult.item_id
                order by consult_timestamp asc
            )
            = 1
    ),

    bookings_origin_last_touch as (
        select
            all_bookings_reconciled.user_id,
            booking_date,
            booking_timestamp,
            booking_session_id,
            booking_unique_session_id,
            reco_call_id,
            all_bookings_reconciled.offer_id,
            all_bookings_reconciled.item_id,
            booking_id,
            consult_date,
            consult_timestamp,
            consult_origin as consult_origin_last_touch,
            platform,
            search_id,
            module_id as module_id_last_touch,
            module_name as module_name_last_touch,
            entry_id as home_id_last_touch
        from all_bookings_reconciled
        inner join
            firebase_consult
            on all_bookings_reconciled.user_id = firebase_consult.user_id
            and all_bookings_reconciled.item_id = firebase_consult.item_id
            and consult_date >= date_sub(booking_date, interval 7 day)
            and consult_timestamp <= timestamp_add(booking_timestamp, interval 5 minute)
        qualify
            row_number() over (
                partition by
                    booking_id, firebase_consult.user_id, firebase_consult.item_id
                order by consult_timestamp desc
            )
            = 1
    ),

    booking_origin as (
        select
            all_bookings_reconciled.user_id,
            all_bookings_reconciled.booking_date,
            all_bookings_reconciled.booking_timestamp,
            all_bookings_reconciled.booking_session_id,
            all_bookings_reconciled.booking_app_version,
            all_bookings_reconciled.booking_unique_session_id,
            all_bookings_reconciled.offer_id,
            all_bookings_reconciled.offer_category_id,
            all_bookings_reconciled.offer_subcategory_id,
            all_bookings_reconciled.item_id,
            all_bookings_reconciled.booking_id,
            all_bookings_reconciled.deposit_id,
            all_bookings_reconciled.booking_status,
            all_bookings_reconciled.booking_is_cancelled,
            all_bookings_reconciled.booking_intermediary_amount,
            first_t.consult_date,
            first_t.consult_timestamp,
            consult_origin_first_touch,
            consult_origin_last_touch,
            first_t.platform,
            first_t.search_id as search_id_first_touch,
            last_t.search_id as search_id_last_touch,
            first_t.reco_call_id as reco_call_id_first_touch,
            last_t.reco_call_id as reco_call_id_last_touch,
            module_id_first_touch,
            module_name_first_touch,
            module_id_last_touch,
            module_name_last_touch,
            home_id_last_touch,
            home_id_first_touch
        from all_bookings_reconciled
        left join bookings_origin_first_touch as first_t using (booking_id)
        left join bookings_origin_last_touch as last_t using (booking_id)
    ),

    mapping_module as (
        select *
        from {{ ref("int_contentful__homepage") }}
        qualify rank() over (partition by module_id, home_id order by date desc) = 1
    )

select
    user_id,
    deposit_id,
    booking_date,
    booking_timestamp,
    booking_session_id,
    booking_app_version,
    booking_unique_session_id,
    offer_id,
    offer_category_id,
    offer_subcategory_id,
    item_id,
    booking_id,
    booking_status,
    booking_is_cancelled,
    booking_intermediary_amount,
    consult_date,
    consult_timestamp,
    -- origin
    consult_origin_first_touch,
    consult_origin_last_touch,
    platform,
    -- technical related
    reco_call_id_first_touch,
    reco_call_id_last_touch,
    search_id_first_touch,
    search_id_last_touch,
    -- home related first_touch
    module_id_first_touch,
    first_touch_map.home_name as home_name_first_touch,
    first_touch_map.content_type as content_type_first_touch,
    home_tag.home_type as home_type_first_touch,
    module_id_last_touch,
    last_touch_map.home_name as home_name_last_touch,
    -- home related last_touch
    last_touch_map.content_type as content_type_last_touch,
    coalesce(
        booking_origin.module_name_first_touch, first_touch_map.module_name
    ) as module_name_first_touch,
    coalesce(home_id_first_touch, first_touch_map.home_id) as home_id_first_touch,
    coalesce(
        booking_origin.module_name_last_touch, last_touch_map.module_name
    ) as module_name_last_touch,
    coalesce(home_id_last_touch, last_touch_map.home_id) as home_id_last_touch

from booking_origin
left join
    mapping_module as first_touch_map
    on booking_origin.module_id_first_touch = first_touch_map.module_id
    and booking_origin.home_id_first_touch = first_touch_map.home_id
left join
    mapping_module as last_touch_map
    on booking_origin.module_id_last_touch = last_touch_map.module_id
    and booking_origin.home_id_last_touch = last_touch_map.home_id
left join
    {{ ref("int_contentful__home_tag") }} as home_tag
    on booking_origin.home_id_first_touch = home_tag.entry_id
