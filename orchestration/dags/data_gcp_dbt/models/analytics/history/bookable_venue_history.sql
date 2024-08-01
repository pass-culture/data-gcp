{{
    config(
        **custom_incremental_config(
        incremental_strategy = 'insert_overwrite',
        partition_by = {'field': 'partition_date', 'data_type': 'date'},
    )
) }}

with all_bookable_data as (
    select
        o.venue_id,
        v.venue_managing_offerer_id as offerer_id,
        partition_date,
        'individual' as offer_type,
        COUNT(distinct offer_id) as nb_bookable_offers
    from {{ ref('bookable_offer_history') }}
        inner join {{ ref('offer') }} as o using (offer_id)
        left join {{ ref('venue') }} as v on o.venue_id = v.venue_id
    {% if is_incremental() %}
        where partition_date = DATE_SUB('{{ ds() }}', interval 1 day)
    {% endif %}
    group by 1, 2, 3, 4
    union all
    select
        venue_id,
        offerer_id,
        partition_date,
        'collective' as offer_type,
        COUNT(distinct collective_offer_id) as nb_bookable_offers
    from {{ ref('bookable_collective_offer_history') }}
        inner join {{ ref('enriched_collective_offer_data') }} using (collective_offer_id)
    {% if is_incremental() %}
        where partition_date = DATE_SUB('{{ ds() }}', interval 1 day)
    {% endif %}
    group by 1, 2, 3, 4
),

pivoted_data as (
    select
        venue_id,
        offerer_id,
        partition_date,
        individual as individual_bookable_offers,
        collective as collective_bookable_offers
    from all_bookable_data
        pivot (SUM(nb_bookable_offers) for offer_type in ('individual', 'collective'))
)

select
    venue_id,
    offerer_id,
    partition_date,
    COALESCE(individual_bookable_offers, 0) as individual_bookable_offers,
    COALESCE(collective_bookable_offers, 0) as collective_bookable_offers,
    COALESCE(individual_bookable_offers, 0) + COALESCE(collective_bookable_offers, 0) as total_bookable_offers
from pivoted_data
