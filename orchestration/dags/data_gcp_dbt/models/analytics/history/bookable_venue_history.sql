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
        DATE('{{ ds() }}') AS partition_date,
        "individual" as offer_type,
        COUNT(CASE
              WHEN DATE_SUB('{{ ds() }}', interval 1 day)  >= DATE(dbt_valid_from)
                   AND (dbt_valid_to IS NULL OR DATE_SUB('{{ ds() }}', interval 1 day) <= DATE(dbt_valid_to))
              THEN offer_id
              ELSE NULL
         END) AS total_bookable_offers
    from {{ ref('snapshot__bookable_offer') }}
        inner join {{ source('raw','applicative_database_offer') }} as o using (offer_id)
        inner join {{ source('raw','applicative_database_venue')  }} as v using (venue_id)
    group by venue_id,
        offerer_id,
        partition_date,
        offer_type
    union all
    select
        o.venue_id,
        venue_managing_offerer_id as offerer_id,
        DATE('{{ ds() }}') AS partition_date,
        "collective" as offer_type,
        COUNT(CASE
              WHEN DATE_SUB('{{ ds() }}', interval 1 day) >= DATE(dbt_valid_from)
                   AND (dbt_valid_to IS NULL OR DATE_SUB('{{ ds() }}', interval 1 day) <= DATE(dbt_valid_to))
              THEN collective_offer_id
              ELSE NULL
         END) AS total_bookable_offers
    from {{ ref('snapshot__bookable_collective_offer') }}
        inner join {{ source('raw','applicative_database_collective_offer') }} as o using (collective_offer_id)
        inner join {{ source('raw','applicative_database_venue')  }} as v using (venue_id)
    group by venue_id,
        offerer_id,
        partition_date,
        offer_type
),

pivoted_data as (
    select
        venue_id,
        offerer_id,
        partition_date,
        individual as total_individual_bookable_offers,
        collective as total_collective_bookable_offers
    from all_bookable_data
        pivot (SUM(total_bookable_offers) for offer_type in ('individual', 'collective'))
)

select
    venue_id,
    offerer_id,
    partition_date,
    COALESCE(total_individual_bookable_offers, 0) as total_individual_bookable_offers,
    COALESCE(total_collective_bookable_offers, 0) as total_collective_bookable_offers,
    COALESCE(total_individual_bookable_offers, 0) + COALESCE(total_collective_bookable_offers, 0) as total_bookable_offers
from pivoted_data
