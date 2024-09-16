{% snapshot snapshot__bookable_offer %}
    {{
        config(
          unique_key='offer_id',
          strategy='check',
          check_cols=['offer_is_bookable'],
        )
    }}

    SELECT
        offer_id,
        offer_is_bookable,
        current_timestamp() AS snapshot_at
    FROM {{ ref('int_global__offer') }}

{% endsnapshot %}