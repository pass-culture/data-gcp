select
    module_displayed_date,
    entry_id as home_id,
    entry_name as home_name,
    parent_module_id,
    parent_module_name,
    parent_module_type,
    coalesce(user_role, 'Grand Public') as user_role,
    count(distinct unique_session_id) as nb_sesh_display,
    count(
        distinct case
            when
                consult_offer_timestamp is not null
                or click_type is not null
                or consult_venue_timestamp is not null
            then unique_session_id
            else null
        end
    ) as nb_sesh_click,
    count(
        distinct case
            when consult_offer_timestamp is not null then unique_session_id else null
        end
    ) as nb_sesh_consult_offer,
    count(
        distinct case
            when click_type = 'ConsultVideo' then unique_session_id else null
        end
    ) as nb_sesh_consult_video,
    count(
        case when consult_offer_timestamp is not null then 1 else null end
    ) as nb_consult_offer,
    count(
        distinct case
            when booking_timestamp is not null then unique_session_id else null
        end
    ) as nb_sesh_booking,
    count(case when booking_timestamp is not null then 1 else null end) as nb_bookings,
    count(
        case when booking_id is not null then 1 else null end
    ) as nb_bookings_non_cancelled,
    sum(delta_diversification) as total_diversification
from {{ ref("firebase_home_funnel_conversion") }}
left join {{ ref("int_applicative__user") }} using (user_id)
left join {{ ref("diversification_booking") }} using (booking_id)
group by
    module_displayed_date,
    entry_id,
    entry_name,
    parent_module_id,
    parent_module_name,
    parent_module_type,
    user_role
