CREATE TABLE IF NOT EXISTS {{ dataset }}.{{ tmp_table_name }} ON cluster default
    ENGINE = MergeTree
    PARTITION BY cast(partition_date as Date)
    ORDER BY (user_id, log_timestamp)
    SETTINGS storage_policy='gcs_main'

AS
    SELECT
        cast(partition_date as Date) as partition_date,
        cast(log_timestamp as DateTime) as log_timestamp,
        cast(environement as String) as environement,
        cast(user_id as Nullable(Int64)) as user_id,
        cast(extra_user_id as Nullable(String)) as extra_user_id,
        cast(url_path as Nullable(String)) as url_path,
        cast(status_code as Nullable(Int64)) as status_code,
        cast(device_id as Nullable(String)) as device_id,
        cast(source_ip as Nullable(String)) as source_ip,
        cast(app_version as Nullable(String)) as app_version,
        cast(platform as Nullable(String)) as platform,
        cast(trace as Nullable(String)) as trace,
        cast(technical_message_id as Nullable(String)) as technical_message_id,
        cast(choice_datetime as Nullable(String)) as choice_datetime,
        cast(analytics_source as Nullable(String)) as analytics_source,
        cast(cookies_consent_mandatory as Array(String)) as cookies_consent_mandatory,
        cast(cookies_consent_accepted as Array(String)) as cookies_consent_accepted,
        cast(cookies_consent_refused as Array(String)) as cookies_consent_refused,
        cast(source as Nullable(String)) as source,
        cast(newly_subscribed_themes as Nullable(String)) as newly_subscribed_themes,
        cast(newly_subscribed_email as Nullable(String)) as newly_subscribed_email,
        cast(newly_subscribed_push as Nullable(String)) as newly_subscribed_push,
        cast(currently_subscribed_themes as Nullable(String)) as currently_subscribed_themes,
        cast(currently_subscribed_marketing_push as Nullable(String)) as currently_subscribed_marketing_push,
        cast(currently_subscribed_marketing_email as Nullable(String)) as currently_subscribed_marketing_email,
        cast(newly_unsubscribed_themes as Nullable(String)) as newly_unsubscribed_themes,
        cast(newly_unsubscribed_email as Nullable(String)) as newly_unsubscribed_email,
        cast(newly_unsubscribed_push as Nullable(String)) as newly_unsubscribed_push
    FROM s3(
        gcs_credentials,
        url='{{ bucket_path }}'
)
