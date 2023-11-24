{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    on_schema_change='append_new_columns',
    unique_key= 'booking_id',
    partition_by = {
     'field': 'patition_date', 
     'data_type': 'date',
     'granularity': 'day'
   }
  )
}}
select * from {{ ref('applicative_database_booking') }}
{% if is_incremental() %}
where _PARTITIONDATE >= datetime_sub(current_date(), INTERVAL 1 DAY)
{% else %}
where _PARTITIONDATE < current_date()
{% endif %}