-- Join with firebase_events to get the number of sessions
-- & compute indicators.
-- *** Missing utm
with
    brevo_newsletter as (
        select
            campaign_id,
            campaign_utm,
            campaign_name,
            campaign_target,
            campaign_sent_date,
            share_link,
            update_date,
            audience_size,
            open_number,
            unsubscriptions,
            row_number() over (
                partition by campaign_id order by update_date desc
            ) as rank_update
        from `{{ bigquery_raw_dataset }}.brevo_newsletters_histo`
        qualify rank_update = 1

        union all

        select
            campaign_id,
            campaign_utm,
            campaign_name,
            campaign_target,
            campaign_sent_date,
            share_link,
            update_date,
            audience_size,
            open_number,
            unsubscriptions,
            row_number() over (
                partition by campaign_id order by update_date desc
            ) as rank_update
        from `{{ bigquery_raw_dataset }}.brevo_pro_newsletters_histo`
        qualify rank_update = 1
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
            `{{ bigquery_analytics_dataset }}.global_user_beneficiary` user
            on firebase.user_id = user.user_id
        where
            traffic_campaign is not null
            and event_name
            in ('ConsultOffer', 'BookingConfirmation', 'HasAddedOfferToFavorites')
            and event_date >= date_sub(date("{{ ds }}"), interval 30 day)
            and lower(traffic_medium) like "%email%"
        group by 1, 2
    )

select
    campaign_id,
    campaign_utm,
    campaign_name,
    campaign_sent_date,
    share_link,
    audience_size,
    open_number,
    unsubscriptions,
    user_current_deposit_type,
    session_number,
    offer_consultation_number,
    booking_number,
    favorites_number,
    date(update_date) as update_date

from brevo_newsletter
left join user_traffic on brevo_newsletter.campaign_utm = user_traffic.traffic_campaign
