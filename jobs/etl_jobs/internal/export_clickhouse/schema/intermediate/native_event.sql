CREATE TABLE IF NOT EXISTS intermediate.dev_native_event ON CLUSTER default
(
    partition_date Date,
    event_name String,
    event_timestamp Nullable(DateTime),
    offer_id Nullable(String),
    user_id Nullable(String),
    unique_session_id Nullable(String),
    venue_id Nullable(String),
    origin Nullable(String),
    is_consult_offer UInt8,
    is_consult_venue UInt8,
    is_add_to_favorites UInt8

)
ENGINE = MergeTree
PARTITION BY partition_date
ORDER BY (event_name, IFNULL(venue_id, 'unknown_venue_id'), IFNULL(offer_id, 'unknown_offer_id'))
SETTINGS storage_policy='gcs_main'
COMMENT 'native events'
