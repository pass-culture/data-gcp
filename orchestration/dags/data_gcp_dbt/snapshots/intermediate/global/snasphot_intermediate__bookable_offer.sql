{% snapshot snapshot__bookable_offer %}
    {{
        config(
            unique_key="offer_id",
            strategy="check",
            check_cols=["offer_id"],
        )
    }}

    select offer_id
    from {{ ref("int_global__offer") }}
    where offer_is_bookable

{% endsnapshot %}
