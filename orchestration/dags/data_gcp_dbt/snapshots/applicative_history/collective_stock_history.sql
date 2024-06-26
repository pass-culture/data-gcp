{% snapshot collective_stock_history %}
    
{{
    config(
      strategy='check',
      unique_key='collective_stock_id',
      check_cols=['stock_id', 'collective_stock_creation_date', 'collective_stock_modification_date', 'collective_stock_beginning_date_time', 'collective_offer_id', 'collective_stock_price', 'collective_stock_booking_limit_date_time', 'collective_stock_number_of_tickets']
    )
}}

SELECT
	collective_stock_id,
	stock_id,
	collective_stock_creation_date,
	collective_stock_modification_date,
	collective_stock_beginning_date_time,
	collective_offer_id,
	collective_stock_price,
	collective_stock_booking_limit_date_time,
	collective_stock_number_of_tickets
FROM {{ source('raw', 'applicative_database_collective_stock') }}

{% endsnapshot %}