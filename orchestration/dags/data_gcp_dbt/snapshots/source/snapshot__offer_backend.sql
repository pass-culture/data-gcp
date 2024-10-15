{% snapshot snapshot__offer_backend %}

    {{
        config(
            **custom_snapshot_config(
                strategy="timestamp",
                unique_key="offer_id",
                updated_at="offer_date_updated",
                invalidate_hard_delete=False,
            )
        )
    }}

    select *
    from {{ ref("raw_backend__offer") }}

{% endsnapshot %}
