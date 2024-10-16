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
            stock_id,
            partition_date,
            count(
                distinct case
                    when booking_status not in ('CANCELLED') then booking_id else null
                end
            ) as booking_stock_no_cancelled_cnt
        from {{ source("clean", "applicative_database_booking_history") }} as booking
        {% if is_incremental() %}
            where partition_date = date_sub('{{ ds() }}', interval 1 day)
        {% endif %}
        group by stock_id, partition_date
    )

select distinct
    stock.partition_date,
    stock.offer_id,
    int_applicative__offer_item_id.item_id,
    offer.offer_subcategoryid as offer_subcategory_id,
    subcategories.category_id as offer_category_id
from {{ source("clean", "applicative_database_stock_history") }} as stock
join
    {{ source("clean", "applicative_database_offer_history") }} as offer
    on stock.offer_id = offer.offer_id
    and stock.partition_date = offer.partition_date
    and offer.offer_is_active
    and not stock.stock_is_soft_deleted
left join
    bookings_per_stock
    on stock.stock_id = bookings_per_stock.stock_id
    and stock.partition_date = bookings_per_stock.partition_date
left join
    {{ ref("int_applicative__offer_item_id") }} int_applicative__offer_item_id
    on int_applicative__offer_item_id.offer_id = stock.offer_id
left join
    {{ source("raw", "subcategories") }} subcategories
    on subcategories.id = offer.offer_subcategoryid
where
    (
        (
            date(stock.stock_booking_limit_date) > stock.partition_date
            or stock.stock_booking_limit_date is null
        )
        and (
            date(stock.stock_beginning_date) > stock.partition_date
            or stock.stock_beginning_date is null
        )
        and offer.offer_is_active
        and (
            stock.stock_quantity is null
            or greatest(
                stock.stock_quantity
                - coalesce(bookings_per_stock.booking_stock_no_cancelled_cnt, 0),
                0
            )
            > 0
        )
    )
    {% if is_incremental() %}
        and stock.partition_date = date_sub('{{ ds() }}', interval 1 day)
    {% endif %}
