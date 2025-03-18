{% snapshot snapshot__applicative_collective_stock %}

    {{
        config(
            unique_key="collective_stock_id",
            strategy="timestamp",
            updated_at="collective_stock_modification_date",
        )
    }}

    with
        formated_collective_stock as (
            select
                collective_stock_id,
                collective_stock_creation_date,
                cast(
                    collective_stock_modification_date as timestamp
                ) as collective_stock_modification_date,
                collective_stock_beginning_date_time,
                collective_offer_id,
                collective_stock_price,
                collective_stock_booking_limit_date_time,
                collective_stock_number_of_tickets
            from {{ source("raw", "applicative_database_collective_stock") }}
        )

    select *
    from formated_collective_stock

{% endsnapshot %}
