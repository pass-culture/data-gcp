{{
    config(
        tags = "monthly",
        labels = {'schedule': 'monthly'},
        **custom_incremental_config(
        incremental_strategy='insert_overwrite',
        partition_by={'field': 'month', 'data_type': 'date'},
    )
) }}


with monthly_bookable_items as (
    select
        DATE_TRUNC(partition_date, month) as month,
        item_id,
        offer_subcategory_id,
        offer_category_id,
        COUNT(distinct offer_id) as nb_bookable_offers,
        COUNT(distinct partition_date) as nb_bookable_days
    from {{ ref('bookable_offer_history') }} bookable_offer_history
    {% if is_incremental() %} -- recalculate latest day's DATA + previous
        where
            DATE(partition_date) between
            DATE_TRUNC('{{ ds() }}', month) -- first day of the month
            and
            DATE_SUB(DATE_ADD(DATE_TRUNC('{{ ds() }}', month), interval 1 month), interval 1 day) -- last day of the month
    {% endif %}
    group by 1, 2, 3, 4
),

monthly_consulted_items as (
    select
        DATE_TRUNC(event_date, month) as month,
        item_id,
        SUM(nb_daily_consult) as nb_monthly_consult,
        SUM(case when origin = 'search' then nb_daily_consult else NULL end) as nb_monthly_search_consult,
        SUM(case when origin in ('home', 'video', 'videoModal', 'highlightOffer', 'thematicHighlight', 'exclusivity') then nb_daily_consult else NULL end) as nb_monthly_home_consult,
        SUM(case when origin = 'venue' then nb_daily_consult else NULL end) as nb_monthly_venue_consult,
        SUM(case when origin = 'favorites' then nb_daily_consult else NULL end) as nb_monthly_favorites_consult,
        SUM(case when origin in ('similar_offer', 'same_artist_playlist') then nb_daily_consult else NULL end) as nb_monthly_similar_offer_consult,
        SUM(case when origin not in ('search', 'home', 'video', 'videoModal', 'highlightOffer', 'thematicHighlight', 'exclusivity', 'venue', 'favorites', 'similar_offer', 'same_artist_playlist') then nb_daily_consult else NULL end) as nb_monthly_other_channel_offer_consult
    from {{ ref('firebase_daily_offer_consultation_data') }} firebase_daily_offer_consultation_data
    {% if is_incremental() %} -- recalculate latest day's DATA + previous
        where
            DATE(event_date) between
            DATE_TRUNC('{{ ds() }}', month) -- first day of the month
            and
            DATE_SUB(DATE_ADD(DATE_TRUNC('{{ ds() }}', month), interval 1 month), interval 1 day) -- last day of the month
    {% endif %}
    group by 1, 2
)

select *
from monthly_bookable_items
    left join monthly_consulted_items using (month, item_id)
