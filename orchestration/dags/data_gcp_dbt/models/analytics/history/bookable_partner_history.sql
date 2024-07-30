{{
    config(
        **custom_incremental_config(
        incremental_strategy = 'insert_overwrite',
        partition_by = {'field': 'partition_date', 'data_type': 'date'},
    )
) }}

with all_bookable_data as (
    select
        mrt_global__offer.partner_id,
        partition_date,
        'individual' as offer_type,
        COUNT(distinct offer_id) as nb_bookable_offers
    from {{ ref('bookable_offer_history') }}
        inner join {{ ref('mrt_global__offer') }} as mrt_global__offer using (offer_id)
    {% if is_incremental() %}
        where partition_date = DATE_SUB('{{ ds() }}', interval 1 day)
    {% endif %}
    group by 1, 2, 3
    union all
    select
        enriched_collective_offer_data.partner_id,
        partition_date,
        'collective' as offer_type,
        COUNT(distinct collective_offer_id) as nb_bookable_offers
    from {{ ref('bookable_collective_offer_history') }}
        inner join {{ ref('enriched_collective_offer_data') }} using (collective_offer_id)
    {% if is_incremental() %}
        where partition_date = DATE_SUB('{{ ds() }}', interval 1 day)
    {% endif %}
    group by 1, 2, 3
),

pivoted_data as (
    select
        partner_id,
        partition_date,
        individual as individual_bookable_offers,
        collective as collective_bookable_offers
    from all_bookable_data
        pivot (SUM(nb_bookable_offers) for offer_type in ('individual', 'collective'))
)

select
    partner_id,
    partition_date,
    COALESCE(individual_bookable_offers, 0) as individual_bookable_offers,
    COALESCE(collective_bookable_offers, 0) as collective_bookable_offers,
    COALESCE(individual_bookable_offers, 0) + COALESCE(collective_bookable_offers, 0) as total_bookable_offers
from pivoted_data
