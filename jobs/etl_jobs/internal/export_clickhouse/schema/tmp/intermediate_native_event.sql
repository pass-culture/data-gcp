CREATE TABLE IF NOT EXISTS {{ dataset }}.{{ tmp_table_name }} ON cluster default
    ENGINE = MergeTree
    PARTITION BY partition_date
    ORDER BY (event_name, IFNULL(venue_id, 'unknown_venue_id') ,IFNULL(offer_id, 'unknown_offer_id'))
    SETTINGS storage_policy='gcs_main'

AS
    SELECT
        toDate(partition_date) AS partition_date,
        cast(event_name as String) as event_name,
        cast(event_timestamp AS Nullable(DateTime)) AS event_timestamp,
        cast(offer_id as Nullable(String)) as offer_id,
        cast(user_id as Nullable(String)) as user_id,
        cast(unique_session_id as Nullable(String)) as unique_session_id,
        cast(venue_id as Nullable(String)) as venue_id,
        cast(origin as Nullable(String)) as origin,
        cast(is_consult_venue as UInt8) as is_consult_venue,
        cast(is_consult_offer as UInt8) as is_consult_offer,
        cast(is_add_to_favorites as UInt8) as is_add_to_favorites
    FROM s3(
        gcs_credentials,
        url='{{ bucket_path }}'
)
