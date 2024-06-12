{% snapshot stock_history %}
    
{{
    config(
      strategy='check',
      unique_key='venue_id',
      check_cols=['stock_modified_at_last_provider_date', 'stock_id', 'stock_modified_date', 'stock_price', 'stock_quantity', 'stock_booking_limit_date', 'offer_id', 'stock_is_soft_deleted', 'stock_beginning_date', 'stock_creation_date', 'number_of_tickets']
    )
}}

SELECT
	stock_modified_at_last_provider_date,
	stock_id,
	stock_modified_date,
	stock_price,
	stock_quantity,
	stock_booking_limit_date,
	offer_id,
	stock_is_soft_deleted,
	stock_beginning_date,
	stock_creation_date,
	number_of_tickets
FROM {{ source('raw', 'applicative_database_stock') }}

{% endsnapshot %}