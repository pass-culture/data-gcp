CREATE TABLE IF NOT EXISTS intermediate.native_events ON CLUSTER default
(
    partition_date String,
    event_name String,
    event_timestamp Nullable(String),
    offer_id Nullable(String),
    user_id Nullable(String),
    unique_session_id Nullable(String),
    venue_id Nullable(String),
    origin Nullable(String),
    is_consult_offer UInt64,
    is_consult_venue UInt64,
    is_add_to_favorites UInt64

)
ENGINE = MergeTree
PARTITION BY partition_date
ORDER BY event_name
SETTINGS storage_policy='gcs_main'
COMMENT 'native events'