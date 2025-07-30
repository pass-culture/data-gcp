-- Join with firebase_events to get the number of sessions
-- & compute indicators.
-- *** Missing utm
with
    brevo_transactional as (
        select distinct * from `{{ bigquery_clean_dataset }}.brevo_transactional`
    ),

    user_traffic as (
        select
            traffic_campaign,
            current_deposit_type as user_current_deposit_type,
            count(distinct session_id) as session_number,
            count(
                distinct case
                    when event_name = 'ConsultOffer' then offer_id else null
                end
            ) as offer_consultation_number,
            count(
                distinct case
                    when event_name = 'BookingConfirmation' then booking_id else null
                end
            ) as booking_number,
            count(
                distinct case
                    when event_name = 'HasAddedOfferToFavorites' then offer_id else null
                end
            ) as favorites_number
        from `{{ bigquery_int_firebase_dataset }}.native_event` firebase
        left join
            `{{ bigquery_analytics_dataset }}.global_user` user
            on firebase.user_id = user.user_id
        where
            traffic_campaign is not null
            and lower(traffic_medium) like "%email%"
            and event_name
            in ('ConsultOffer', 'BookingConfirmation', 'HasAddedOfferToFavorites')
            and event_date >= date_sub(date("{{ ds }}"), interval 30 day)
        group by 1, 2
    )

select
    template as template_id,
    tag,
    delivered_count as audience_size,
    unique_opened_count as open_number,
    unsubscribed_count as unsubscriptions,
    user_current_deposit_type,
    session_number,
    offer_consultation_number,
    booking_number,
    favorites_number,
    date(update_date) as update_date

from brevo_transactional
left join user_traffic on brevo_transactional.tag = user_traffic.traffic_campaign
