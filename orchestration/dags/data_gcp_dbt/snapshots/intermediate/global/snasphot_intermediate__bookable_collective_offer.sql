{% snapshot snapshot__bookable_collective_offer %}
    {{
        config(
          unique_key='collective_offer_id',
          strategy='check',
          check_cols=['collective_offer_id'],
          invalidate_hard_deletes=true
        )
    }}

    SELECT
        collective_offer_id
    FROM {{ ref('int_global__collective_offer') }}
    WHERE collective_offer_is_bookable

{% endsnapshot %}