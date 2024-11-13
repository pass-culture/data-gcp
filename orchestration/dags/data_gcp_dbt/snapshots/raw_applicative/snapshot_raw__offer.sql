{% snapshot snapshot_raw__offer %}

    {{
        config(
            **custom_snapshot_config(
                strategy="timestamp",
                unique_key="offer_id",
                updated_at="offer_updated_date",
                invalidate_hard_delete=False,
            )
        )
    }}

    select *
    from {{ ref("raw_applicative__offer_lite") }}

{% endsnapshot %}
