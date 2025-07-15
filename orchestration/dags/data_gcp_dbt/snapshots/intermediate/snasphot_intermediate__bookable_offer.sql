{% snapshot snapshot__bookable_offer %}
    {{
        config(
            unique_key="offer_id",
            strategy="check",
            check_cols=["offer_id"],
            hard_deletes="invalidate",
        )
    }}

    select offer_id
    from {{ ref("int_applicative__offer") }}
    where offer_is_bookable

{% endsnapshot %}
