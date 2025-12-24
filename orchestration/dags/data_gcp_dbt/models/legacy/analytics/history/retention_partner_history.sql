with
    all_activated_partners_and_days_since_activation as (  -- Pour chaque partner_id, une ligne par jour depuis la 1ère offre publiée
        select
            partner_id,
            first_offer_creation_date,
            date_add(date('2022-07-01'), interval offset day) as day  -- Tous les jours depuis le 1er juillet (date à laquelle on a commencé à storer la réservabilité d'une offre / d'un lieu)
        from {{ ref("mrt_global__cultural_partner") }}
        cross join
            unnest(generate_array(0, date_diff(current_date(), '2022-07-01', day))) as
        offset
        where
            date_add(date('2022-07-01'), interval offset day)
            >= first_offer_creation_date  -- Les jours depuis la 1ère offre
            and date_add(date('2022-07-01'), interval offset day) < current_date()  -- Que des jours avant aujourd'hui
    ),

    all_days_and_bookability as (
        select
            all_activated_partners_and_days_since_activation.partner_id,
            first_offer_creation_date,
            day,
            coalesce(total_bookable_offers, 0) as total_bookable_offers
        from all_activated_partners_and_days_since_activation
        left join
            {{ ref("bookable_partner_history") }}
            on bookable_partner_history.partner_id
            = all_activated_partners_and_days_since_activation.partner_id
            and bookable_partner_history.partition_date
            = all_activated_partners_and_days_since_activation.day
    )

select
    partner_id,
    first_offer_creation_date,
    day,
    total_bookable_offers,
    max(case when total_bookable_offers != 0 then day end) over (
        partition by partner_id
        order by day
        rows between unbounded preceding and current row
    ) as last_bookable_date,
    date_diff(
        day,
        max(case when total_bookable_offers != 0 then day end) over (
            partition by partner_id
            order by day
            rows between unbounded preceding and current row
        ),
        day
    ) as days_since_last_bookable_date
from all_days_and_bookability
