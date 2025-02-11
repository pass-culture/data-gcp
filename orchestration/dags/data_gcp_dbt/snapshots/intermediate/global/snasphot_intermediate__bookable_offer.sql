{% snapshot snapshot__bookable_offer %}
    {{
        config(
            unique_key="offer_id",
            strategy="check",
            check_cols=["offer_is_bookable"],
        )
    }}

    select offer_id, offer_is_bookable
    from {{ ref("int_applicative__offer") }}

{% endsnapshot %}
