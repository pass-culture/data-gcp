{% snapshot booking_snapshot %}

{{
    config(
      target_schema='snapshots_applicative',
      strategy='check',
      unique_key='booking_id',
      check_cols=['booking_status']
    )
}}
    

select * from {{ source('raw', 'applicative_database_booking') }}

{% endsnapshot %}


