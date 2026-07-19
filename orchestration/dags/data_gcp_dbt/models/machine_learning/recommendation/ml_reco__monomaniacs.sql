{% set excluded_subcategories = ["MATERIEL_ART_CREATIF", "CARTE_CINE_MULTISEANCES"] %}
{% set min_booking_cnt = 5 %}
{% set lookback_months = 24 %}

with
    booking_events as (
        select
            b.user_id,
            b.offer_id,
            cast(b.user_age as int64) as user_age,
            o.item_id,
            o.offer_subcategory_id,
            o.offer_category_id,
            o.genres,
            o.rayon,
            o.type
        from {{ ref("mrt_global__booking") }} as b
        inner join {{ ref("mrt_global__offer") }} as o on b.offer_id = o.offer_id
        where
            b.booking_creation_date
            >= date_sub(date("{{ ds() }}"), interval {{ lookback_months }} month)
            and o.offer_subcategory_id
            not in ('{{ excluded_subcategories | join("', '") }}')
    ),

    monomaniacs as (
        select
            user_id,
            count(*) as nbr_booking,
            count(distinct offer_category_id) as distinct_categories,
            count(distinct offer_subcategory_id) as distinct_subcategories,
            count(distinct genres) as distinct_genres,
            count(distinct rayon) as distinct_rayon,
            count(distinct type) as distinct_types
        from booking_events
        group by user_id
        having
            nbr_booking > {{ min_booking_cnt }}
            and distinct_categories <= 1
            and distinct_subcategories <= 1
            and distinct_genres <= 1
            and distinct_rayon <= 1
            and distinct_types <= 1
    ),

    monomaniac_bookings as (
        select
            b.*,
            m.nbr_booking,
            m.distinct_categories,
            m.distinct_subcategories,
            m.distinct_genres,
            m.distinct_rayon,
            m.distinct_types
        from booking_events as b
        inner join monomaniacs as m using (user_id)
    ),

    last_visit_stats as (
        select
            v.user_id,
            v.user_engagement_level,
            v.user_last_week_engagement_level,
            v.nb_co_last_4_weeks,
            v.nb_co_3_months_ago,
            v.nb_co_2_months_ago,
            v.nb_co_last_3_months,
            v.nb_visits,
            v.nb_distinct_days_visits,
            v.nb_visits_marketing,
            v.nb_consult_offer,
            v.nb_booking_confirmation,
            v.nb_add_to_favorites
        from {{ ref("aggregated_weekly_user_data") }} as v
        inner join monomaniacs as m on m.user_id = v.user_id
        qualify
            row_number() over (partition by v.user_id order by v.visit_rank desc) = 1
    )

select
    mono.*,
    visit.* except (user_id),
    u.user_density_label,
    u.user_macro_density_label,
    u.user_is_active,
    meta.gtl_type,
    meta.gtl_label_level_1,
    meta.gtl_label_level_2,
    meta.gtl_label_level_3,
    meta.gtl_label_level_4
from monomaniac_bookings as mono
left join {{ ref("ml_input__item_metadata") }} as meta using (item_id)
left join {{ ref("mrt_global__user_beneficiary") }} as u using (user_id)
left join last_visit_stats as visit using (user_id)
where
    u.user_is_active
    and mono.user_age >= 18
    and mono.user_age <= 20
    and (
        visit.user_engagement_level is not null
        or visit.user_last_week_engagement_level != "Dead user"
    )
order by mono.user_id
