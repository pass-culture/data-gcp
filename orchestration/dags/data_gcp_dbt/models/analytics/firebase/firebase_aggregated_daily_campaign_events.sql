{{
  config(
    **custom_incremental_config(
    partition_by={
      "field": "event_date",
      "data_type": "date",
      "granularity": "day",
      "time_ingestion_partitioning": false
    },
    incremental_strategy = "insert_overwrite",
    on_schema_change = "sync_all_columns"
  )
) }}

with bookings_and_diversification_per_sesh as (
    select
        firebase_bookings.unique_session_id,
        COUNT(distinct booking_id) as booking_diversification_cnt,
        SUM(delta_diversification) as total_delta_diversification
    from {{ ref('firebase_bookings') }} firebase_bookings
        inner join {{ ref('diversification_booking') }} diversification_booking using (booking_id)
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
    COALESCE(daily_activity.deposit_type, 'Grand Public') as user_type,
    COUNT(distinct firebase_visits.unique_session_id) as nb_sesh,
    COUNT(distinct case when nb_consult_offer > 0 then firebase_visits.unique_session_id else NULL end) as nb_sesh_consult,
    COUNT(distinct case when nb_add_to_favorites > 0 then firebase_visits.unique_session_id else NULL end) as nb_sesh_add_to_fav,
    COUNT(distinct case when nb_booking_confirmation > 0 then firebase_visits.unique_session_id else NULL end) as nb_sesh_booking,
    COALESCE(SUM(nb_consult_offer), 0) as nb_consult_offer,
    COALESCE(SUM(nb_add_to_favorites), 0) as nb_add_to_favorites,
    COALESCE(SUM(nb_booking_confirmation), 0) as nb_booking,
    COALESCE(SUM(booking_diversification_cnt), 0) as nb_non_cancelled_bookings,
    COALESCE(SUM(total_delta_diversification), 0) as total_delta_diversification,
    COUNT(distinct case when nb_signup_completed > 0 then firebase_visits.unique_session_id else NULL end) as nb_signup,
    COUNT(distinct case when nb_benef_request_sent > 0 then firebase_visits.unique_session_id else NULL end) as nb_benef_request_sent
from {{ ref('firebase_visits') }} firebase_visits
    inner join
        {{ ref('firebase_session_origin') }}
            firebase_session_origin
        on firebase_session_origin.unique_session_id = firebase_visits.unique_session_id
            and firebase_session_origin.traffic_campaign is not NULL
    left join
        {{ ref('aggregated_daily_user_used_activity') }}
            daily_activity
        on daily_activity.user_id = firebase_visits.user_id
            and daily_activity.active_date = DATE(firebase_visits.first_event_timestamp)
    left join bookings_and_diversification_per_sesh on bookings_and_diversification_per_sesh.unique_session_id = firebase_visits.unique_session_id
{% if is_incremental() %}
    where firebase_session_origin.first_event_date between DATE_SUB(DATE('{{ ds() }}'), interval 1 day) and DATE('{{ ds() }}')
{% endif %}
group by 1, 2, 3, 4, 5, 6, 7, 8
