{% snapshot snapshot__bookable_collective_offer %}
    {{
        config(
            unique_key="collective_offer_id",
            strategy="check",
            check_cols=["collective_offer_is_bookable"],
        )
    }}

    select collective_offer_id, collective_offer_is_bookable
    from {{ ref("int_applicative__collective_offer") }}

{% endsnapshot %}
