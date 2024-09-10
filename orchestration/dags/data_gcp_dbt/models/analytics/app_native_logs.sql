{{
    config(
        **custom_incremental_config(
        incremental_strategy='insert_overwrite',
        partition_by={'field': 'partition_date', 'data_type': 'date'},
    )
) }}

select
    DATE(timestamp) as partition_date,
    timestamp,
    COALESCE(CAST(jsonpayload.user_id as STRING), CAST(jsonpayload.extra.user_id as STRING)) as user_id,
    jsonpayload.technical_message_id,
    jsonpayload.extra.choice_datetime,
    jsonpayload.extra.device_id,
    jsonpayload.extra.analyticssource,
    jsonpayload.extra.consent.mandatory as cookies_consent_mandatory,
    jsonpayload.extra.consent.accepted as cookies_consent_accepted,
    jsonpayload.extra.consent.refused as cookies_consent_refused,
    ARRAY_TO_STRING(jsonpayload.extra.newlysubscribedto.themes, ' - ') as newly_subscribed_themes,
    CAST(jsonpayload.extra.newlysubscribedto.email as STRING) as newly_subscribed_email,
    CAST(jsonpayload.extra.newlysubscribedto.push as STRING) as newly_subscribed_push,
    ARRAY_TO_STRING(jsonpayload.extra.subscriptions.subscribed_themes, ' - ') as currently_subscribed_themes,
    CAST(jsonpayload.extra.subscriptions.marketing_push as STRING) as currently_subscribed_marketing_push,
    CAST(jsonpayload.extra.subscriptions.marketing_email as STRING) as currently_subscribed_marketing_email,
    ARRAY_TO_STRING(jsonpayload.extra.newlyunsubscribedfrom.themes, ' - ') as newly_unsubscribed_themes,
    CAST(jsonpayload.extra.newlyunsubscribedfrom.email as STRING) as newly_unsubscribed_email,
    CAST(jsonpayload.extra.newlyunsubscribedfrom.push as STRING) as newly_unsubscribed_push
from
    {{ source("raw","stdout") }}
where
    1 = 1
    {% if is_incremental() %}
        and DATE(timestamp) = "{{ ds }}"
    {% endif %}
    and jsonpayload.extra.analyticssource = "app-native"
