{{
    config(
        tags="monthly",
        labels={"schedule": "monthly"},
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "month", "data_type": "date"},
        )
    )
}}


with
    monthly_bookable_items as (
        select
            date_trunc(partition_date, month) as month,
            item_id,
            offer_subcategory_id,
            offer_category_id,
            count(distinct offer_id) as nb_bookable_offers,
            count(distinct partition_date) as nb_bookable_days
        from {{ ref("int_history__bookable_offer") }}
        {% if is_incremental() %}  -- recalculate latest day's DATA + previous
            where
                date(partition_date)
                between date_trunc('{{ ds() }}', month)  -- first day of the month
                and date_sub(
                    date_add(date_trunc('{{ ds() }}', month), interval 1 month),
                    interval 1 day
                )  -- last day of the month
        {% endif %}
        group by 1, 2, 3, 4
    ),

    monthly_consulted_items as (
        select
            date_trunc(event_date, month) as month,
            item_id,
            sum(nb_daily_consult) as nb_monthly_consult,
            sum(
                case when origin = 'search' then nb_daily_consult end
            ) as nb_monthly_search_consult,
            sum(
                case
                    when
                        origin in (
                            'home',
                            'video',
                            'videoModal',
                            'highlightOffer',
                            'thematicHighlight',
                            'exclusivity'
                        )
                    then nb_daily_consult
                end
            ) as nb_monthly_home_consult,
            sum(
                case when origin = 'venue' then nb_daily_consult end
            ) as nb_monthly_venue_consult,
            sum(
                case when origin = 'favorites' then nb_daily_consult end
            ) as nb_monthly_favorites_consult,
            sum(
                case
                    when origin in ('similar_offer', 'same_artist_playlist')
                    then nb_daily_consult
                end
            ) as nb_monthly_similar_offer_consult,
            sum(
                case
                    when
                        origin not in (
                            'search',
                            'home',
                            'video',
                            'videoModal',
                            'highlightOffer',
                            'thematicHighlight',
                            'exclusivity',
                            'venue',
                            'favorites',
                            'similar_offer',
                            'same_artist_playlist'
                        )
                    then nb_daily_consult
                end
            ) as nb_monthly_other_channel_offer_consult
        from
            {{ ref("firebase_daily_offer_consultation_data") }}
            as firebase_daily_offer_consultation_data
        {% if is_incremental() %}  -- recalculate latest day's DATA + previous
            where
                date(event_date)
                between date_trunc('{{ ds() }}', month)  -- first day of the month
                and date_sub(
                    date_add(date_trunc('{{ ds() }}', month), interval 1 month),
                    interval 1 day
                )  -- last day of the month
        {% endif %}
        group by 1, 2
    )

select *
from monthly_bookable_items
left join monthly_consulted_items using (month, item_id)
