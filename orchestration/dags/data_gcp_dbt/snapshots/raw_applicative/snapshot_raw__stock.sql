{% snapshot snapshot_raw__stock %}

    {{
        config(
            **custom_snapshot_config(
                strategy="timestamp",
                unique_key="stock_id",
                updated_at="stock_modified_date",
                invalidate_hard_delete=false,
            )
        )
    }}

    select *
    from {{ ref("raw_applicative__stock_lite") }}

{% endsnapshot %}
