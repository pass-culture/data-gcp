{% snapshot snapshot_raw__event_series_offer_link %}
    {{
        config(
            **custom_snapshot_config(
                strategy="timestamp",
                unique_key="event_series_offer_link_id",
                updated_at="date_modified",
                hard_deletes="invalidate"

            )
        )
    }}

    select
        event_series_offer_link_id,
        event_series_id,
        offer_id,
        date_created,
        date_modified
    from {{ source("raw", "applicative_database_event_series_offer_link") }}

{% endsnapshot %}
