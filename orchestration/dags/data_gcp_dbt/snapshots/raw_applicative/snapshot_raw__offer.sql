{% snapshot snapshot_raw__offer %}

    {% if is_first_day_of_month() == "TRUE" %}

        {{
            config(
                **custom_snapshot_config(
                    unique_key="offer_id",
                    strategy="check",
                    check_cols=["custom_scd_id"],
                )
            )
        }}

        select *
        from {{ ref("raw_applicative__offer_full") }}

    {% else %}

        {{
            config(
                **custom_snapshot_config(
                    strategy="timestamp",
                    unique_key="offer_id",
                    updated_at="offer_updated_date",
                    invalidate_hard_deletes=False,
                )
            )
        }}

        select *
        from {{ ref("raw_applicative__offer_lite") }}

    {% endif %}

{% endsnapshot %}
