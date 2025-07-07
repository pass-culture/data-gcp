{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_date", "data_type": "date"},
            on_schema_change="append_new_columns",
            require_partition_filter=true,
        )
    )
}}

select
    partition_date,
    log_timestamp,
    environement,
    user_id,
    extra_user_id,
    url_path,
    status_code,
    device_id,
    source_ip,
    app_version,
    platform,
    trace,
    technical_message_id,
    choice_datetime,
    analytics_source,
    cookies_consent_mandatory,
    cookies_consent_accepted,
    cookies_consent_refused,
    case
        url_path
        when "/native/v1/me"
        then "app_native"
        when "/beneficiaries/current"
        then "webapp"
        when "/users/current"
        then "pro"
    end as source,
    newly_subscribed_themes,
    newly_subscribed_email,
    newly_subscribed_push,
    currently_subscribed_themes,
    currently_subscribed_marketing_push,
    currently_subscribed_marketing_email,
    newly_unsubscribed_themes,
    newly_unsubscribed_email,
    newly_unsubscribed_push
from {{ ref("int_pcapi__log") }}
where
    log_timestamp >= date_sub(current_timestamp(), interval 365 day)
    and url_path in ("/users/current", "/native/v1/me", "/native/v1/signin")
    {% if is_incremental() %}
        and date(log_timestamp)
        between date_sub(date("{{ ds() }}"), interval 2 day) and date("{{ ds() }}")
        and date(partition_date)
        between date_sub(date("{{ ds() }}"), interval 2 day) and date("{{ ds() }}")
    {% endif %}
