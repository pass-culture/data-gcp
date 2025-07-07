CREATE TABLE IF NOT EXISTS {{ dataset }}.{{ tmp_table_name }} ON cluster default
    ENGINE = MergeTree
    PARTITION BY venue_id
    ORDER BY update_date
    SETTINGS storage_policy='gcs_main'

AS
    SELECT
        '{{ date }}' as update_date,
        cast(venue_id as String) as venue_id,
        cast(total_active_offers as UInt64) as total_active_offers,
        cast(total_pending_offers as UInt64) as total_pending_offers,
        cast(total_inactive_non_rejected_offers as UInt64) as total_inactive_non_rejected_offers,
        cast(total_active_collective_offers as UInt64) as total_active_collective_offers,
        cast(total_pending_collective_offers as UInt64) as total_pending_collective_offers,
        cast(total_inactive_non_rejected_collective_offers as UInt64) as total_inactive_non_rejected_collective_offers
    FROM s3(
        gcs_credentials,
        url='{{ bucket_path }}'
)
