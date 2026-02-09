{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_date", "data_type": "date"},
        )
    )
}}


with
    {% if not is_incremental() %}
        date_bounds as (
            select
                date('{{ var("PASS_START_DATE") }}') as start_date,
                date('{{ ds() }}') as end_date
        ),
    {% endif %}

    dates as (
        {% if is_incremental() %}select date('{{ ds() }}') as partition_date
        {% else %}
            select day as partition_date
            from
                date_bounds,
                unnest(generate_date_array(start_date, end_date, interval 1 day)) as day
        {% endif %}
    ),

    all_bookable_data as (
        select
            o.venue_id,
            v.venue_managing_offerer_id as offerer_id,
            d.partition_date,
            'individual' as offer_type,
            count(distinct s.offer_id) as total_bookable_offers
        from dates as d
        inner join
            {{ ref("snapshot__bookable_offer") }} as s
            on date(s.dbt_valid_from) <= d.partition_date
            and (s.dbt_valid_to is null or date(s.dbt_valid_to) > d.partition_date)
        inner join {{ ref("int_applicative__offer") }} as o using (offer_id)
        inner join
            {{ source("raw", "applicative_database_venue") }} as v using (venue_id)
        group by venue_id, offerer_id, partition_date, offer_type

        union all

        select
            o.venue_id,
            v.venue_managing_offerer_id as offerer_id,
            d.partition_date,
            'collective' as offer_type,
            count(distinct sb.collective_offer_id) as total_bookable_offers
        from dates as d
        inner join
            {{ ref("snapshot__bookable_collective_offer") }} as sb
            on date(sb.dbt_valid_from) <= d.partition_date
            and (sb.dbt_valid_to is null or date(sb.dbt_valid_to) > d.partition_date)
        inner join
            {{ ref("int_applicative__collective_offer") }} as o using (
                collective_offer_id
            )
        inner join
            {{ source("raw", "applicative_database_venue") }} as v using (venue_id)
        group by venue_id, offerer_id, partition_date, offer_type
    ),

    pivoted_data as (
        select
            venue_id,
            offerer_id,
            partition_date,
            coalesce(individual, 0) as total_individual_bookable_offers,
            coalesce(collective, 0) as total_collective_bookable_offers
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
    total_individual_bookable_offers,
    total_collective_bookable_offers,
    total_individual_bookable_offers
    + total_collective_bookable_offers as total_bookable_offers
from pivoted_data
