with
    brevo_perf_by_user as (
        select
            tag,
            template,
            user_id,
            event_date,
            sum(delivered_count) as delivered_count,
            sum(opened_count) as opened_count,
            sum(unsubscribed_count) as unsubscribed_count
        from {{ source("raw", "sendinblue_transactional") }}
        where tag like 'jeune%'
        group by tag, template, user_id, event_date
    ),

    user_traffic as (
        select
            firebase.user_id,
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
        from {{ ref("int_firebase__native_event") }} firebase
        left join {{ ref("mrt_global__user") }} user on firebase.user_id = user.user_id
        where
            traffic_campaign is not null
            and lower(traffic_medium) like "%email%"
            and event_name
            in ('ConsultOffer', 'BookingConfirmation', 'HasAddedOfferToFavorites')
            and event_date >= date_sub(date("{{ ds() }}"), interval 30 day)
        group by 1, 2, 3
    )

select
    brevo_perf_by_user.user_id,
    template as template_id,
    tag,
    event_date,
    delivered_count,
    opened_count,
    unsubscribed_count,
    user_current_deposit_type,
    session_number,
    offer_consultation_number,
    booking_number,
    favorites_number
from brevo_perf_by_user
left join
    user_traffic
    on brevo_perf_by_user.user_id = user_traffic.user_id
    and brevo_perf_by_user.tag = user_traffic.traffic_campaign
