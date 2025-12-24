with
    partner_crea_frequency as (
        select
            mrt_global__offer.partner_id,
            count(
                distinct date_trunc(offer_creation_date, month)
            ) as nb_mois_crea_this_year
        from {{ ref("mrt_global__offer") }} as mrt_global__offer
        where date_diff(current_date, offer_creation_date, month) <= 12
        group by 1
    ),

    cultural_sector_crea_frequency as (
        select distinct
            mrt_global__cultural_partner.partner_type,
            percentile_disc(nb_mois_crea_this_year, 0.5) over (
                partition by mrt_global__cultural_partner.partner_type
            ) as median_crea_offer_frequency
        from partner_crea_frequency
        inner join
            {{ ref("mrt_global__cultural_partner") }} as mrt_global__cultural_partner
            using (partner_id)
    ),

    partner_bookability_frequency as (
        select
            partner_id,
            count(
                distinct date_trunc(partition_date, month)
            ) as nb_mois_bookable_this_year
        from {{ ref("bookable_partner_history") }}
        where date_diff(current_date, partition_date, month) <= 12
        group by 1
    ),

    cultural_sector_bookability_frequency as (
        select distinct
            mrt_global__cultural_partner.partner_type,
            percentile_disc(nb_mois_bookable_this_year, 0.5) over (
                partition by mrt_global__cultural_partner.partner_type
            ) as median_bookability_frequency
        from partner_bookability_frequency
        inner join
            {{ ref("mrt_global__cultural_partner") }} as mrt_global__cultural_partner
            using (partner_id)
    )

select
    partner_type,
    median_bookability_frequency,
    median_crea_offer_frequency,
    case
        when median_bookability_frequency <= 6
        then 1
        when median_bookability_frequency >= 11
        then 3
        else 2
    end as cultural_sector_bookability_frequency_group
from cultural_sector_crea_frequency
left join cultural_sector_bookability_frequency using (partner_type)
order by 1
