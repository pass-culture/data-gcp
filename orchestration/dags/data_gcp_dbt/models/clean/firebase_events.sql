{{
    config(
        materialized='incremental',
        partition_by = {'field': 'event_date_parsed', 'data_type': 'date'},
        incremental_strategy = 'insert_overwrite',
        on_schema_change='fail'
    )
}}

select *, PARSE_DATE('%Y%m%d', event_date) as event_date_parsed
 from {{ source('clean', 'firebase_events') }}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses >= to include records arriving later on the same day as the last run of this model)
  where PARSE_DATE('%Y%m%d', event_date) >= (select max(PARSE_DATE('%Y%m%d', event_date)) from {{ this }})

{% endif %}