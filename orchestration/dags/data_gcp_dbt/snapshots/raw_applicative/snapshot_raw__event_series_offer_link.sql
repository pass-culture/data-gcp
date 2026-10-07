{% snapshot snapshot_raw__event_series_offer_link %}
    {{
        config(
            **custom_snapshot_config(
                strategy="timestamp",
                unique_key="event_series_offer_link_id",
                updated_at="modified_at",
                hard_deletes="invalidate"

            )
        )
    }}

    select
        event_series_offer_link_id,
        event_series_id,
        offer_id,
        date_created,
        date_modified,
        cast(date_modified as timestamp) as modified_at
    from {{ source("raw", "applicative_database_event_series_offer_link") }}

{% endsnapshot %}
