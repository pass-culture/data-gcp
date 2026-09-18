{% snapshot snapshot_raw__event_series %}

    {{
        config(
            **custom_snapshot_config(
                strategy="timestamp",
                unique_key="event_series_id",
                updated_at="date_modified",
                hard_deletes="invalidate"

            )
        )
    }}
    select
        event_series_id,
        event_series_name,
        event_series_description,
        event_series_mediation_uuid,
        date_created,
        date_modified
    from {{ source("raw", "applicative_database_event_series") }}

{% endsnapshot %}
