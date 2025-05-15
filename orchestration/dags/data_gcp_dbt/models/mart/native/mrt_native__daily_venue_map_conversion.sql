{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={
                "field": "event_date",
                "data_type": "date",
                "granularity": "day",
            },
            on_schema_change="append_new_columns",
        )
    )
}}

with
    venue_map_consultations as (  -- Toutes les consultations d'offres
        -- consulted from venue
        select
            offer.unique_session_id,
            offer.user_id,
            offer.event_date,
            offer.event_timestamp,
            offer.app_version,
            offer.event_name,
            offer.origin,
            offer.venue_id,
            offer.offer_id,
            offer.duration_seconds
        from {{ ref("int_firebase__native_venue_map_event") }} as offer
        inner join
            {{ ref("int_firebase__native_venue_map_event") }} as preview using (
                unique_session_id, venue_id
            )
        where
            offer.event_name = 'ConsultOffer'
            and offer.origin = 'venue'
            and preview.event_name = 'ConsultVenue'
            and preview.event_timestamp < offer.event_timestamp
            {% if is_incremental() %}
                and offer.event_date
                between date_sub(date("{{ ds() }}"), interval 1 day) and date(
                    "{{ ds() }}"
                )
            {% endif %}
        qualify
            row_number() over (
                partition by unique_session_id, offer_id order by event_timestamp
            )
            = 1
        union all
        -- consulted directly from preview
        select
            unique_session_id,
            user_id,
            event_date,
            event_timestamp,
            app_version,
            event_name,
            origin,
            venue_id,
            offer_id,
            duration_seconds
        from {{ ref("int_firebase__native_venue_map_event") }}
        where
            event_name = 'ConsultOffer' and origin in ('venuemap', 'venueMap')
            {% if is_incremental() %}
                and event_date
                between date_sub(date("{{ ds() }}"), interval 1 day) and date(
                    "{{ ds() }}"
                )
            {% endif %}
        qualify
            row_number() over (
                partition by unique_session_id, offer_id order by event_timestamp
            )
            = 1
    ),

    venue_map_bookings as (  -- Les réservations
        select
            unique_session_id,
            ne.user_id,
            ne.event_date,
            ne.event_timestamp,
            ne.app_version,
            ne.event_name,
            ne.origin,
            ne.venue_id,
            ne.offer_id,
            db.diversity_score as delta_diversity,
            safe_cast(ne.duration as int64) as duration_seconds
        from {{ ref("int_firebase__native_event") }} as ne
        inner join venue_map_consultations using (unique_session_id, offer_id, user_id)
        left join
            {{ ref("mrt_global__booking") }} as db on ne.booking_id = db.booking_id
        where
            ne.event_name = 'BookingConfirmation'
            and venue_map_consultations.event_name = 'ConsultOffer'
            and ne.event_timestamp > venue_map_consultations.event_timestamp
            {% if is_incremental() %}
                and ne.event_date
                between date_sub(date("{{ ds() }}"), interval 1 day) and date(
                    "{{ ds() }}"
                )
            {% endif %}
    ),

    all_events as (  -- On reprend les events "enrichis"
        select
            unique_session_id,
            user_id,
            event_date,
            event_timestamp,
            app_version,
            event_name,
            origin,
            venue_id,
            offer_id,
            duration_seconds,
            null as delta_diversity
        from {{ ref("int_firebase__native_venue_map_event") }}
        where
            event_name in (
                'ConsultVenueMap',
                'VenueMapSessionDuration',
                'VenueMapSeenDuration',
                'PinMapPressed',
                'ConsultVenue'
            )
            {% if is_incremental() %}
                and event_date
                between date_sub(date("{{ ds() }}"), interval 1 day) and date(
                    "{{ ds() }}"
                )
            {% endif %}
        union all
        select
            unique_session_id,
            user_id,
            event_date,
            event_timestamp,
            app_version,
            event_name,
            origin,
            venue_id,
            offer_id,
            duration_seconds,
            null as delta_diversity
        from venue_map_consultations
        union all
        select
            unique_session_id,
            user_id,
            event_date,
            event_timestamp,
            app_version,
            event_name,
            origin,
            venue_id,
            offer_id,
            duration_seconds,
            delta_diversity
        from venue_map_bookings
    )

select
    unique_session_id,
    user_id,
    app_version,
    min(event_date) as event_date,
    count(
        case when event_name = 'ConsultVenueMap' then 1 end
    ) as total_venue_map_consult,
    count(
        distinct case when event_name = 'PinMapPressed' then venue_id end
    ) as total_venue_map_preview,
    count(
        distinct case when event_name = 'ConsultVenue' then venue_id end
    ) as total_consult_venue,
    count(
        distinct case when event_name = 'ConsultOffer' then venue_id end
    ) as total_distinct_venue_consult_offer,
    count(
        distinct case when event_name = 'ConsultOffer' then offer_id end
    ) as total_consult_offer,
    count(
        distinct case when event_name = 'BookingConfirmation' then offer_id end
    ) as total_bookings,
    count(
        distinct case
            when
                event_name = 'BookingConfirmation' and delta_diversity is not null
            then offer_id
        end
    ) as total_non_cancelled_bookings,
    sum(coalesce(delta_diversity, 0)) as total_diversity,
    sum(
        case when event_name = 'VenueMapSeenDuration' then duration_seconds end
    ) as total_session_venue_map_seen_duration_seconds
from all_events
group by unique_session_id, user_id, app_version
