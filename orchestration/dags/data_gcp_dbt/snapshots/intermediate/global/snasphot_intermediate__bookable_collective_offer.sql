{% snapshot snapshot__bookable_collective_offer %}
    {{
        config(
            unique_key="collective_offer_id",
            strategy="check",
            check_cols=["collective_offer_id"],
            invalidate_hard_deletes=True,
        )
    }}

    select collective_offer_id
    from {{ ref("int_applicative__collective_offer") }}
    where collective_offer_is_bookable

{% endsnapshot %}
