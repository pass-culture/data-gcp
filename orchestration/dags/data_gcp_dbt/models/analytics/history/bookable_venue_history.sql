{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_date", "data_type": "date"},
        )
    )
}}


with
    all_bookable_data as (
        select
            o.venue_id,
            v.venue_managing_offerer_id as offerer_id,
            date('{{ ds() }}') as partition_date,
            'individual' as offer_type,
            count(
                case
                    when
                        date_sub('{{ ds() }}', interval 1 day) >= date(s.dbt_valid_from)
                        and (
                            s.dbt_valid_to is null
                            or date_sub('{{ ds() }}', interval 1 day)
                            <= date(s.dbt_valid_to)
                        )
                    then s.offer_id
                end
            ) as total_bookable_offers
        from {{ ref("snapshot__bookable_offer") }} as s
        inner join {{ ref("int_applicative__offer") }} as o using (offer_id)
        inner join
            {{ source("raw", "applicative_database_venue") }} as v using (venue_id)
        group by
            o.venue_id, v.venue_managing_offerer_id, date('{{ ds() }}'), 'individual'
        union all
        select
            o.venue_id,
            v.venue_managing_offerer_id as offerer_id,
            date('{{ ds() }}') as partition_date,
            'collective' as offer_type,
            count(
                case
                    when
                        date_sub('{{ ds() }}', interval 1 day)
                        >= date(sb.dbt_valid_from)
                        and (
                            sb.dbt_valid_to is null
                            or date_sub('{{ ds() }}', interval 1 day)
                            <= date(sb.dbt_valid_to)
                        )
                    then sb.collective_offer_id
                end
            ) as total_bookable_offers
        from {{ ref("snapshot__bookable_collective_offer") }} as sb
        inner join
            {{ source("raw", "applicative_database_collective_offer") }} as o using (
                collective_offer_id
            )
        inner join
            {{ source("raw", "applicative_database_venue") }} as v using (venue_id)
        group by
            o.venue_id, v.venue_managing_offerer_id, date('{{ ds() }}'), 'collective'
    ),

    pivoted_data as (
        select
            venue_id,
            offerer_id,
            partition_date,
            individual as total_individual_bookable_offers,
            collective as total_collective_bookable_offers
        from
            all_bookable_data pivot (
                sum(total_bookable_offers) for offer_type
                in ('individual', 'collective')
            )
    )

select
    venue_id,
    offerer_id,
    partition_date,
    coalesce(total_individual_bookable_offers, 0) as total_individual_bookable_offers,
    coalesce(total_collective_bookable_offers, 0) as total_collective_bookable_offers,
    coalesce(total_individual_bookable_offers, 0)
    + coalesce(total_collective_bookable_offers, 0) as total_bookable_offers
from pivoted_data
