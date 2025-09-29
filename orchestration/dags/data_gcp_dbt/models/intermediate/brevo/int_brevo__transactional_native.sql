-- noqa: disable=all
with
    brevo_perf_by_user as (
        select
            tag,
            template,
            target,
            user_id,
            event_date,
            sum(delivered_count) as total_delivered,
            sum(opened_count) as total_opened,
            sum(unsubscribed_count) as total_unsubscribed
        from {{ source("raw", "sendinblue_transactional") }}
        where target = 'native'
        group by tag, template, target, user_id, event_date
    ),

    user_traffic as (
        select
            firebase.user_id,
            traffic_campaign,
            current_deposit_type as user_current_deposit_type,
            count(distinct session_id) as session_number,
            count(
                distinct case when event_name = 'ConsultOffer' then offer_id end
            ) as total_consultations,
            count(
                distinct case
                    when event_name = 'BookingConfirmation' then booking_id
                end
            ) as total_bookings,
            count(
                distinct case
                    when event_name = 'HasAddedOfferToFavorites' then offer_id
                end
            ) as total_favorites
        from {{ ref("int_firebase__native_event") }} as firebase
        left join
            {{ ref("mrt_global__user_beneficiary") }} as user
            on firebase.user_id = user.user_id
        where
            traffic_campaign is not null
            and lower(traffic_medium) like '%email%'
            and event_name
            in ('ConsultOffer', 'BookingConfirmation', 'HasAddedOfferToFavorites')
            and event_date >= date_sub(date("{{ ds() }}"), interval 30 day)
        group by 1, 2, 3
    )

select
    brevo_perf_by_user.user_id,
    template as brevo_template_id,
    tag as brevo_tag,
    target,
    event_date,
    total_delivered,
    total_opened,
    total_unsubscribed,
    user_current_deposit_type,
    session_number,
    total_consultations,
    total_bookings,
    total_favorites
from brevo_perf_by_user
left join
    user_traffic
    on brevo_perf_by_user.user_id = user_traffic.user_id
    and brevo_perf_by_user.tag = user_traffic.traffic_campaign
