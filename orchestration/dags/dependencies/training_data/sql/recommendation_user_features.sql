with
    user_qpi as (
        select
            user_id,
            string_agg(
                distinct subcategories order by subcategories
            ) as qpi_subcategory_ids
        from `{{ bigquery_analytics_dataset }}`.enriched_qpi_answers
        where subcategories <> 'none'
        group by user_id
    )
select
    firebase_agg.user_id,
    firebase_agg.consult_offer,
    stats_reco.booking_cnt,
    round(
        stats_reco.user_theoretical_remaining_credit, 0
    ) as user_theoretical_remaining_credit,
    stats_reco.has_added_offer_to_favorites,
    string_agg(distinct firebase.query, " ") as user_queries,
    uqpi.qpi_subcategory_ids as qpi_subcategory_ids

from `{{ bigquery_analytics_dataset }}`.firebase_aggregated_users firebase_agg
join
    `{{ bigquery_ml_reco_dataset }}`.user_statistics stats_reco
    on stats_reco.user_id = firebase_agg.user_id
join
    `{{ bigquery_int_firebase_dataset }}`.native_event firebase
    on firebase.user_id = firebase_agg.user_id
left join user_qpi uqpi on uqpi.user_id = firebase_agg.user_id
where firebase.event_date >= date_sub(current_date, interval 6 month)
group by
    firebase_agg.user_id,
    firebase_agg.consult_offer,
    stats_reco.booking_cnt,
    stats_reco.user_theoretical_remaining_credit,
    stats_reco.has_added_offer_to_favorites,
    uqpi.qpi_subcategory_ids
