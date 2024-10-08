with partner_crea_frequency as (
    select
        partner_id,
        COUNT(distinct DATE_TRUNC(offer_creation_date, month)) as nb_mois_crea_this_year
    from {{ ref('mrt_global__offer') }}
    where DATE_DIFF(CURRENT_DATE, offer_creation_date, month) <= 12
    group by 1
),

cultural_sector_crea_frequency as (
    select distinct
        cultural_sector,
        PERCENTILE_DISC(nb_mois_crea_this_year, 0.5) over (partition by cultural_sector) as median_crea_offer_frequency
    from partner_crea_frequency
        inner join {{ ref('mrt_global__cultural_partner') }} using (partner_id)
),

partner_bookability_frequency as (
    select
        partner_id,
        COUNT(distinct DATE_TRUNC(partition_date, month)) as nb_mois_bookable_this_year
    from {{ ref('bookable_partner_history') }}
    where DATE_DIFF(CURRENT_DATE, partition_date, month) <= 12
    group by 1
),

cultural_sector_bookability_frequency as (
    select distinct
        cultural_sector,
        PERCENTILE_DISC(nb_mois_bookable_this_year, 0.5) over (partition by cultural_sector) as median_bookability_frequency
    from partner_bookability_frequency
        inner join {{ ref('mrt_global__cultural_partner') }} using (partner_id)
)

select
    cultural_sector,
    median_bookability_frequency,
    median_crea_offer_frequency,
    case
        when median_bookability_frequency <= 6 then 1
        when median_bookability_frequency >= 11 then 3
        else 2
    end as cultural_sector_bookability_frequency_group
from cultural_sector_crea_frequency
    left join cultural_sector_bookability_frequency using (cultural_sector)
