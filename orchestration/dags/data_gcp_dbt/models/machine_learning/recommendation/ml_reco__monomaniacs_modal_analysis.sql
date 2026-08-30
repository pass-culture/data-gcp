with
    base as (
        select
            user_id,
            max(user_age) as user_age,
            max(user_density_label) as user_density_label,
            max(user_macro_density_label) as user_macro_density_label,
            max(user_is_active) as user_is_active,
            max(user_engagement_level) as user_engagement_level,
            max(user_last_week_engagement_level) as user_last_week_engagement_level,
            max(nb_co_last_4_weeks) as nb_co_last_4_weeks,
            max(nb_co_3_months_ago) as nb_co_3_months_ago,
            max(nb_co_2_months_ago) as nb_co_2_months_ago,
            max(nb_co_last_3_months) as nb_co_last_3_months,
            max(nb_visits) as nb_visits,
            max(nb_distinct_days_visits) as nb_distinct_days_visits,
            max(nb_visits_marketing) as nb_visits_marketing,
            max(nb_consult_offer) as nb_consult_offer,
            max(nb_booking_confirmation) as nb_booking_confirmation,
            max(nb_add_to_favorites) as nb_add_to_favorites,
            max(distinct_categories) as distinct_categories,
            max(distinct_subcategories) as distinct_subcategories,
            max(distinct_genres) as distinct_genres,
            max(distinct_rayon) as distinct_rayon,
            max(distinct_types) as distinct_types,
            max(nbr_booking) as total_nbr_booking

        from {{ ref("ml_reco__monomaniacs") }}
        group by user_id
    ),

    -- Most represented value (mode) for each categorical column
    modes as (
        select
            cte.user_id,
            array_agg(cte.offer_category_id order by cte.cnt_category desc limit 1)[
                offset(0)
            ] as top_offer_category_id,
            array_agg(
                cte.offer_subcategory_id order by cte.cnt_subcategory desc limit 1
            )[offset(0)] as top_offer_subcategory_id,
            array_agg(cte.genres order by cte.cnt_genres desc limit 1)[
                offset(0)
            ] as top_genres,
            array_agg(cte.rayon order by cte.cnt_rayon desc limit 1)[
                offset(0)
            ] as top_rayon,
            array_agg(cte.type order by cte.cnt_type desc limit 1)[
                offset(0)
            ] as top_type,
            array_agg(cte.gtl_type order by cte.cnt_gtl_type desc limit 1)[
                offset(0)
            ] as top_gtl_type,
            array_agg(cte.gtl_label_level_1 order by cte.cnt_gtl_l1 desc limit 1)[
                offset(0)
            ] as top_gtl_label_level_1,
            array_agg(cte.gtl_label_level_2 order by cte.cnt_gtl_l2 desc limit 1)[
                offset(0)
            ] as top_gtl_label_level_2,
            array_agg(cte.gtl_label_level_3 order by cte.cnt_gtl_l3 desc limit 1)[
                offset(0)
            ] as top_gtl_label_level_3,
            array_agg(cte.gtl_label_level_4 order by cte.cnt_gtl_l4 desc limit 1)[
                offset(0)
            ] as top_gtl_label_level_4

        from
            (
                select
                    m.user_id,
                    m.offer_category_id,
                    m.offer_subcategory_id,
                    m.genres,
                    m.rayon,
                    m.type,
                    m.gtl_type,
                    m.gtl_label_level_1,
                    m.gtl_label_level_2,
                    m.gtl_label_level_3,
                    m.gtl_label_level_4,
                    count(*) over (
                        partition by m.user_id, m.offer_category_id
                    ) as cnt_category,
                    count(*) over (
                        partition by m.user_id, m.offer_subcategory_id
                    ) as cnt_subcategory,
                    count(*) over (partition by m.user_id, m.genres) as cnt_genres,
                    count(*) over (partition by m.user_id, m.rayon) as cnt_rayon,
                    count(*) over (partition by m.user_id, m.type) as cnt_type,
                    count(*) over (partition by m.user_id, m.gtl_type) as cnt_gtl_type,
                    count(*) over (
                        partition by m.user_id, m.gtl_label_level_1
                    ) as cnt_gtl_l1,
                    count(*) over (
                        partition by m.user_id, m.gtl_label_level_2
                    ) as cnt_gtl_l2,
                    count(*) over (
                        partition by m.user_id, m.gtl_label_level_3
                    ) as cnt_gtl_l3,
                    count(*) over (
                        partition by m.user_id, m.gtl_label_level_4
                    ) as cnt_gtl_l4
                from {{ ref("ml_reco__monomaniacs") }} as m
            ) as cte
        group by cte.user_id
    )

select
    b.*,
    m.top_offer_category_id,
    m.top_offer_subcategory_id,
    m.top_genres,
    m.top_rayon,
    m.top_type,
    m.top_gtl_type,
    m.top_gtl_label_level_1,
    m.top_gtl_label_level_2,
    m.top_gtl_label_level_3,
    m.top_gtl_label_level_4
from base as b
left join modes as m using (user_id)
order by b.user_id
