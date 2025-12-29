{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "display_date", "data_type": "date"},
        )
    )
}}

with
    venue_data as (
        select
            user_id,
            unique_session_id,
            event_name,
            event_date,
            event_timestamp,
            origin,
            offer_id,
            booking_id,
            venue_id,
            app_version
        from {{ ref("int_firebase__native_event") }}
        where
            (
                event_name in ('ConsultVenue', 'BookingConfirmation')
                or (event_name = 'ConsultOffer' and origin = 'venue')
            )
            and unique_session_id is not null
            {% if is_incremental() %}
                -- recalculate latest day's data + previous
                and date(event_date)
                >= date_sub(date(_dbt_max_partition), interval 1 day)
            {% endif %}
    ),

    display as (
        select
            unique_session_id,
            event_timestamp as display_timestamp,
            event_date as display_date,
            origin as display_origin,
            venue_id,
            row_number() over (
                partition by unique_session_id, venue_id order by event_timestamp
            ) as venue_display_rank
        from venue_data
        where event_name = 'ConsultVenue'
        qualify
            row_number() over (
                partition by unique_session_id, venue_id order by event_timestamp
            )
            = 1  -- keep first_display
    ),

    consult_offer as (
        select
            display.* except (venue_display_rank),
            offer_id,
            event_timestamp as consult_offer_timestamp,
            row_number() over (
                partition by display.unique_session_id, display.venue_id, offer_id
                order by event_timestamp
            ) as consult_rank
        from display
        left join
            venue_data
            on display.unique_session_id = venue_data.unique_session_id
            and display.venue_id = venue_data.venue_id
            and event_name = 'ConsultOffer'
            and venue_data.event_timestamp > display_timestamp
        qualify
            row_number() over (
                partition by unique_session_id, venue_id, offer_id
                order by event_timestamp
            )
            = 1  -- keep 1 consult max

    )

select
    consult_offer.* except (consult_rank),
    venue_data.booking_id,
    event_timestamp as booking_timestamp
from consult_offer
left join
    venue_data
    on consult_offer.unique_session_id = venue_data.unique_session_id
    and consult_offer.offer_id = venue_data.offer_id
    and consult_offer.consult_offer_timestamp < venue_data.event_timestamp
    and event_name = 'BookingConfirmation'
{% if is_incremental() %}
    where
        display_date
        between date_sub(date('{{ ds() }}'), interval 1 day) and date('{{ ds() }}')
{% endif %}
