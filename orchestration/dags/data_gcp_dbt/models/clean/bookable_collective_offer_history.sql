{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_date", "data_type": "date"},
            require_partition_filter=true,
        )
    )
}}


with
    bookings_per_stock as (
        select
            collective_stock_id,
            partition_date,
            count(
                distinct case
                    when collective_booking_status not in ('CANCELLED')
                    then collective_booking_id
                end
            ) as collective_booking_stock_no_cancelled_cnt
        from
            {{ source("clean", "applicative_database_collective_booking_history") }}
        {% if is_incremental() %}
            where partition_date = date_sub('{{ ds() }}', interval 1 day)
        {% else %}
            where partition_date > date_sub('{{ ds() }}', interval 3 month)
        {% endif %}

        group by 1, 2
    )

select distinct
    collective_stock.collective_offer_id,
    collective_stock.partition_date,
    false as collective_offer_is_template
from
    {{ source("clean", "applicative_database_collective_stock_history") }}
    as collective_stock
inner join
    {{ source("clean", "applicative_database_collective_offer_history") }}
    as collective_offer
    on collective_stock.collective_offer_id = collective_offer.collective_offer_id
    and collective_offer.collective_offer_is_active
    and collective_stock.partition_date = collective_offer.partition_date
    and collective_offer_validation = 'APPROVED'
left join
    bookings_per_stock
    on collective_stock.collective_stock_id = bookings_per_stock.collective_stock_id
    and collective_stock.partition_date = bookings_per_stock.partition_date
where
    (
        (
            date(collective_stock.collective_stock_booking_limit_date_time)
            > collective_stock.partition_date
            or collective_stock.collective_stock_booking_limit_date_time is null
        )
        and (
            date(collective_stock.collective_stock_beginning_date_time)
            > collective_stock.partition_date
            or collective_stock.collective_stock_beginning_date_time is null
        )
        and collective_offer.collective_offer_is_active
        and (collective_booking_stock_no_cancelled_cnt is null)
    )
    {% if is_incremental() %}
        and collective_stock.partition_date = date_sub('{{ ds() }}', interval 1 day)
    {% else %}
        and collective_stock.partition_date > date_sub('{{ ds() }}', interval 3 month)
    {% endif %}

union all
select
    collective_offer_template.collective_offer_id,
    collective_offer_template.partition_date,
    true as collective_offer_is_template
from
    {{ source("clean", "applicative_database_collective_offer_template_history") }}
    as collective_offer_template
where
    collective_offer_validation = 'APPROVED'
    {% if is_incremental() %}
        and partition_date = date_sub('{{ ds() }}', interval 1 day)
    {% else %}
        and partition_date > date_sub('{{ ds() }}', interval 3 month)
    {% endif %}
