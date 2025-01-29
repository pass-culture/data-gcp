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
            partition_date,
            'individual' as offer_type,
            count(distinct offer_id) as nb_bookable_offers
        from {{ ref("bookable_offer_history") }}
        inner join {{ ref("int_applicative__offer") }} as o using (offer_id)
        left join
            {{ source("raw", "applicative_database_venue") }} as v
            on o.venue_id = v.venue_id
        {% if is_incremental() %}
            where partition_date = date_sub('{{ ds() }}', interval 1 day)
        {% endif %}
        group by 1, 2, 3, 4
        union all
        select
            venue_id,
            venue_managing_offerer_id as offerer_id,
            partition_date,
            'collective' as offer_type,
            count(distinct collective_offer_id) as nb_bookable_offers
        from {{ ref("bookable_collective_offer_history") }}
        inner join
            {{ source("raw", "applicative_database_collective_offer") }} using (
                collective_offer_id
            )
        inner join {{ source("raw", "applicative_database_venue") }} using (venue_id)
        where
            collective_offer_validation = 'APPROVED'
            {% if is_incremental() %}
                and partition_date = date_sub('{{ ds() }}', interval 1 day)
            {% endif %}
        group by 1, 2, 3, 4
        union all
        select
            venue_id,
            venue_managing_offerer_id as offerer_id,
            partition_date,
            'collective' as offer_type,
            count(distinct collective_offer_id) as nb_bookable_offers
        from {{ ref("bookable_collective_offer_history") }}
        inner join
            {{ source("raw", "applicative_database_collective_offer_template") }}
            using (collective_offer_id)
        inner join {{ source("raw", "applicative_database_venue") }} using (venue_id)
        where
            collective_offer_validation = 'APPROVED'
            {% if is_incremental() %}
                and partition_date = date_sub('{{ ds() }}', interval 1 day)
            {% endif %}
        group by 1, 2, 3, 4
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
                sum(nb_bookable_offers) for offer_type
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
