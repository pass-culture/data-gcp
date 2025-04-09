with
    display_data as (  -- Séparer les données de display et de conversion
        select *
        from {{ ref("firebase_similar_offer_events") }}
        where event_type = 'display' and user_id is not null and session_id is not null
    ),

    convert_data as (
        select *
        from {{ ref("firebase_similar_offer_events") }}
        where event_type = 'convert' and user_id is not null and session_id is not null
    ),

    display_and_convert as (
        select
            display_data.user_id,
            display_data.session_id,
            display_data.unique_session_id,
            display_data.event_timestamp,
            display_data.event_date,
            mrt_global__user.current_deposit_type,
            display_data.app_version,
            display_data.similar_offer_playlist_type,
            display_data.is_algolia_recommend,
            display_data.reco_call_id,
            display_data.item_id,
            display_data.similar_item_id,
            display_data.user_location_type,
            count(
                distinct case
                    when convert_data.event_name = 'ConsultOffer'
                    then convert_data.item_id
                end
            ) as nb_items_consulted,
            count(
                distinct case
                    when convert_data.event_name = 'BookingConfirmation'
                    then convert_data.item_id
                end
            ) as nb_items_booked,
            sum(
                case
                    when convert_data.event_name = 'BookingConfirmation'
                    then booking.diversity_score
                end
            ) as diversification_score
        from display_data
        left join
            convert_data
            on display_data.unique_session_id = convert_data.unique_session_id
            and display_data.item_id = convert_data.similar_item_id
            and display_data.similar_offer_playlist_type
            = convert_data.similar_offer_playlist_type
        left join
            {{ ref("mrt_global__booking") }} as booking
            on convert_data.booking_id = booking.booking_id
        inner join
            {{ ref("mrt_global__user") }} as mrt_global__user
            on display_data.user_id = mrt_global__user.user_id
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
    ),

    data_and_lags as (  -- Déterminer si un utilisateur a consulté une offre X, vu l'algo d'offres similaires A, consulté depuis A une offre Y, vu l'algo B et consulté depuis B une offre Z
        select
            *,
            lag(similar_item_id) over (
                partition by user_id, session_id order by event_timestamp desc
            ) as lag_1,
            lag(similar_item_id, 2) over (
                partition by user_id, session_id order by event_timestamp desc
            ) as lag_2,
            lag(similar_item_id, 3) over (
                partition by user_id, session_id order by event_timestamp desc
            ) as lag_3
        from display_and_convert
        order by 1, 2, 3
    )

select distinct
    data_and_lags.* except (lag_1, lag_2, lag_3, event_timestamp),
    coalesce(
        item_id = lag_1 or item_id = lag_2 or item_id = lag_3, false
    ) as looped_to_other_offer
from data_and_lags
