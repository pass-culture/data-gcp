with
    user_qpi as (
        select
            user_id,
            string_agg(
                distinct subcategories order by subcategories
            ) as qpi_subcategory_ids
        from {{ ref("enriched_qpi_answers") }}
        where subcategories <> 'none'
        group by user_id
    )

select
    firebase_agg.user_id,
    firebase_agg.consult_offer,
    stats_reco.booking_cnt,
    stats_reco.has_added_offer_to_favorites,
    uqpi.qpi_subcategory_ids,
    round(
        stats_reco.user_theoretical_remaining_credit, 0
    ) as user_theoretical_remaining_credit,
    string_agg(distinct firebase.query, ' ') as user_queries

from {{ ref("firebase_aggregated_users") }} as firebase_agg
inner join
    {{ ref("ml_reco__user_statistics") }} as stats_reco
    on firebase_agg.user_id = stats_reco.user_id
inner join
    {{ ref("int_firebase__native_event") }} as firebase
    on firebase_agg.user_id = firebase.user_id
left join user_qpi as uqpi on firebase_agg.user_id = uqpi.user_id
where firebase.event_date >= date_sub(current_date, interval 6 month)
group by
    firebase_agg.user_id,
    firebase_agg.consult_offer,
    stats_reco.booking_cnt,
    stats_reco.user_theoretical_remaining_credit,
    stats_reco.has_added_offer_to_favorites,
    uqpi.qpi_subcategory_ids
