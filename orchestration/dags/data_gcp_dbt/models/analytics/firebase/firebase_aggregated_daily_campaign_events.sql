{{
    config(
        **custom_incremental_config(
            partition_by={
                "field": "event_date",
                "data_type": "date",
                "granularity": "day",
                "time_ingestion_partitioning": false,
            },
            incremental_strategy="insert_overwrite",
            on_schema_change="append_new_columns",
        )
    )
}}

with
    bookings_and_diversification_per_sesh as (
        select
            firebase_bookings.unique_session_id,
            count(distinct booking_id) as booking_diversification_cnt,
            sum(delta_diversification) as total_delta_diversification
        from {{ ref("firebase_bookings") }} as firebase_bookings
        inner join
            {{ ref("diversification_booking") }} as diversification_booking using (
                booking_id
            )
        group by 1
    )

select
    firebase_session_origin.traffic_campaign,
    firebase_session_origin.traffic_medium,
    firebase_session_origin.traffic_source,
    firebase_session_origin.traffic_gen,
    firebase_session_origin.traffic_content,
    firebase_session_origin.first_event_date as event_date,
    firebase_visits.platform,
    coalesce(daily_activity.deposit_type, 'Grand Public') as user_type,
    count(distinct firebase_visits.unique_session_id) as nb_sesh,
    count(
        distinct case
            when nb_consult_offer > 0 then firebase_visits.unique_session_id
        end
    ) as nb_sesh_consult,
    count(
        distinct case
            when nb_add_to_favorites > 0 then firebase_visits.unique_session_id
        end
    ) as nb_sesh_add_to_fav,
    count(
        distinct case
            when nb_booking_confirmation > 0 then firebase_visits.unique_session_id
        end
    ) as nb_sesh_booking,
    coalesce(sum(nb_consult_offer), 0) as nb_consult_offer,
    coalesce(sum(nb_add_to_favorites), 0) as nb_add_to_favorites,
    coalesce(sum(nb_booking_confirmation), 0) as nb_booking,
    coalesce(sum(booking_diversification_cnt), 0) as nb_non_cancelled_bookings,
    coalesce(sum(total_delta_diversification), 0) as total_delta_diversification,
    count(
        distinct case
            when nb_signup_completed > 0 then firebase_visits.unique_session_id
        end
    ) as nb_signup,
    count(
        distinct case
            when nb_benef_request_sent > 0 then firebase_visits.unique_session_id
        end
    ) as nb_benef_request_sent
from {{ ref("firebase_visits") }} as firebase_visits
inner join
    {{ ref("firebase_session_origin") }} as firebase_session_origin
    on firebase_session_origin.unique_session_id = firebase_visits.unique_session_id
    and firebase_session_origin.traffic_campaign is not null
left join
    {{ ref("mrt_native__daily_user_deposit") }} as daily_activity
    on daily_activity.user_id = firebase_visits.user_id
    and daily_activity.deposit_active_date = date(firebase_visits.first_event_timestamp)
    {% if is_incremental() %}
        and daily_activity.deposit_active_date
        between date_sub(date('{{ ds() }}'), interval 1 day) and date('{{ ds() }}')
    {% endif %}
    and daily_activity.deposit_active_date
    between date_sub(date('{{ ds() }}'), interval 48 month) and date('{{ ds() }}')
left join
    bookings_and_diversification_per_sesh
    on bookings_and_diversification_per_sesh.unique_session_id
    = firebase_visits.unique_session_id
{% if is_incremental() %}
    where
        firebase_session_origin.first_event_date
        between date_sub(date('{{ ds() }}'), interval 1 day) and date('{{ ds() }}')
{% endif %}
group by 1, 2, 3, 4, 5, 6, 7, 8
