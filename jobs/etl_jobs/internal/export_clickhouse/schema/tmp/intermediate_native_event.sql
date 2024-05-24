CREATE TABLE IF NOT EXISTS {{ dataset }}.{{ tmp_table_name }} ON cluster default
    ENGINE = MergeTree
    PARTITION BY partition_date
    ORDER BY event_name
    SETTINGS storage_policy='gcs_main'

AS 
SELECT
    cast(partition_date as String) AS partition_date,
    cast(event_name as String) as event_name,
    cast(event_timestamp as Nullable(String)) as event_timestamp,
    cast(offer_id as Nullable(String)) as offer_id,
    cast(user_id as Nullable(String)) as user_id,
    cast(unique_session_id as Nullable(String)) as unique_session_id,
    cast(venue_id as Nullable(String)) as venue_id,
    cast(origin as Nullable(String)) as origin,
    cast(is_consult_venue as UInt64) as is_consult_venue,
    cast(is_consult_offer as UInt64) as is_consult_offer,
    cast(is_add_to_favorites as UInt64) as is_add_to_favorites

FROM s3(
    gcs_credentials,
    url='{{ bucket_path }}'
)