{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "event_date", "data_type": "date"},
            on_schema_change="append_new_columns",
            cluster_by="reco_call_id",
            require_partition_filter=true,
        )
    )
}}

with

    aggreg as (
        select
            event_date,
            platform,
            unique_session_id,
            reco_config_id,
            reco_call_id,
            playlist_origin,
            context,
            retrieval,
            model_params_name,
            model_params_description,
            scorer_retrieval_model_display_name,
            scorer_ranking_model_display_name,
            offer_category,
            user_id,
            sum(offer_viewed) as views,
            sum(offer_consulted) as consultations,
            sum(offer_faved) as favorites,
            sum(offer_booked) as bookings,
            sum(discovery_score) as discovery,
            sum(diversity_score) as diversity,
            sum(item_niche_score) as sum_item_niche_score,
            sum(offer_niche_score) as sum_offer_niche_score,
            count(distinct offer_id) as reco_weight,
            avg(offer_booking_number) as avg_booking,
            stddev(offer_booking_number) as std_booking
        from {{ ref("int_pcreco__displayed_offer_conversion") }}
        {% if is_incremental() %}
            where
                event_date
                between date_sub(date('{{ ds() }}'), interval 3 day) and date(
                    '{{ ds() }}'
                )
        {% else %} where event_date >= date_sub(date('{{ ds() }}'), interval 60 day)
        {% endif %}
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
    )

select distinct
    a.*,
    u.user_age,
    u.user_is_priority_public,
    u.user_is_in_education,
    u.user_is_in_qpv,
    u.user_density_label,
    u.user_density_level,
    if(
        count(distinct a.offer_category) over (partition by a.reco_call_id) = 1,
        true,
        false
    ) as is_unique_cat_reco
from aggreg as a
left join {{ ref("mrt_global__user_beneficiary") }} as u on a.user_id = u.user_id
