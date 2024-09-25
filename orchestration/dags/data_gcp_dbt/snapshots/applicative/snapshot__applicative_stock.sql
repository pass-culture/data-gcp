{% snapshot snapshot__applicative_stock %}

{{
    config(
      strategy='timestamp',
      unique_key='stock_id',
      updated_at='stock_modified_date'
    )
}}

    with formated_stock as (
        select
            stock_modified_at_last_provider_date,
            stock_id,
            cast(stock_modified_date as timestamp) as stock_modified_date,
            stock_price,
            stock_quantity,
            stock_booking_limit_date,
            offer_id,
            stock_is_soft_deleted,
            stock_beginning_date,
            stock_creation_date
        from {{ source('raw', 'applicative_database_stock') }}
    )

    select * from formated_stock

{% endsnapshot %}
