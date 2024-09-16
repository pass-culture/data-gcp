{% snapshot snapshot__bookable_offer %}
    {{
        config(
          unique_key='offer_id',
          strategy='check',
          check_cols=['offer_is_bookable'],
          invalidate_hard_deletes=true
        )
    }}

    SELECT
        offer_id
        current_timestamp() AS snapshot_at
    FROM {{ ref('int_global__offer') }}
    WHERE offer_is_bookable

{% endsnapshot %}