CREATE TABLE IF NOT EXISTS intermediate.native_log ON CLUSTER default
(
    partition_date Date CODEC(ZSTD),
    log_timestamp DateTime CODEC(ZSTD),
    environement String CODEC(ZSTD),
    user_id Nullable(Int64) CODEC(ZSTD),
    extra_user_id Nullable(String) CODEC(ZSTD),
    url_path Nullable(String) CODEC(ZSTD),
    status_code Nullable(Int64) CODEC(ZSTD),
    device_id Nullable(String) CODEC(ZSTD),
    source_ip Nullable(String) CODEC(ZSTD),
    app_version Nullable(String) CODEC(ZSTD),
    platform Nullable(String) CODEC(ZSTD),
    trace Nullable(String) CODEC(ZSTD),
    technical_message_id Nullable(String) CODEC(ZSTD),
    choice_datetime Nullable(String) CODEC(ZSTD),
    analytics_source Nullable(String) CODEC(ZSTD),
    cookies_consent_mandatory Array(String) CODEC(ZSTD),
    cookies_consent_accepted Array(String) CODEC(ZSTD),
    cookies_consent_refused Array(String) CODEC(ZSTD),
    source Nullable(String) CODEC(ZSTD),
    newly_subscribed_themes Nullable(String) CODEC(ZSTD),
    newly_subscribed_email Nullable(String) CODEC(ZSTD),
    newly_subscribed_push Nullable(String) CODEC(ZSTD),
    currently_subscribed_themes Nullable(String) CODEC(ZSTD),
    currently_subscribed_marketing_push Nullable(String) CODEC(ZSTD),
    currently_subscribed_marketing_email Nullable(String) CODEC(ZSTD),
    newly_unsubscribed_themes Nullable(String) CODEC(ZSTD),
    newly_unsubscribed_email Nullable(String) CODEC(ZSTD),
    newly_unsubscribed_push Nullable(String) CODEC(ZSTD)
)
ENGINE = MergeTree
PARTITION BY partition_date
ORDER BY (user_id, log_timestamp)
SETTINGS storage_policy='gcs_main'
COMMENT 'native logs'
