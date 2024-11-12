{% snapshot snapshot__bookable_collective_offer %}
    {{
        config(
            unique_key="collective_offer_id",
            strategy="check",
            check_cols=["collective_offer_id"],
        )
    }}

    select collective_offer_id
    from {{ ref("int_global__collective_offer") }}
    where collective_offer_is_bookable

{% endsnapshot %}
