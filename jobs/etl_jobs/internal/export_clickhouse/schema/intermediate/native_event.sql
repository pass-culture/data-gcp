CREATE TABLE IF NOT EXISTS intermediate.native_event ON CLUSTER default
(
    partition_date Date CODEC(ZSTD),
    event_name String CODEC(ZSTD),
    event_timestamp Nullable(DateTime) CODEC(Delta, ZSTD),
    offer_id Nullable(String) CODEC(ZSTD),
    user_id Nullable(String) CODEC(ZSTD),
    unique_session_id Nullable(String) CODEC(ZSTD),
    venue_id Nullable(String) CODEC(ZSTD),
    origin Nullable(String) CODEC(ZSTD),
    is_consult_venue UInt8 CODEC(LZ4),
    is_consult_offer UInt8 CODEC(LZ4),
    is_add_to_favorites UInt8 CODEC(LZ4)
)
ENGINE = MergeTree
PARTITION BY partition_date
ORDER BY (event_name, IFNULL(venue_id, 'unknown_venue_id'), IFNULL(offer_id, 'unknown_offer_id'))
SETTINGS storage_policy='gcs_main'
COMMENT 'native events'
