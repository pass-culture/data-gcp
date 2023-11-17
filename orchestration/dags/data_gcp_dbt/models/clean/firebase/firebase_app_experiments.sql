{{
    config(
        materialized='incremental',
        partition_by = {'field': 'event_date', 'data_type': 'date'},
        incremental_strategy = 'insert_overwrite',
        on_schema_change='fail'
    )
}}

select * from {{ source('clean', 'firebase_app_experiments') }}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses >= to include records arriving later on the same day as the last run of this model)
  where event_date >= (select max(event_date) from {{ this }})

{% endif %}