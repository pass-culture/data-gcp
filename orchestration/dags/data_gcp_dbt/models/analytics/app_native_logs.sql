{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_date", "data_type": "date"},
        )
    )
}}

select
    date(timestamp) as partition_date,
    timestamp,
    coalesce(
        cast(jsonpayload.user_id as string), cast(jsonpayload.extra.user_id as string)
    ) as user_id,
    jsonpayload.technical_message_id,
    jsonpayload.extra.choice_datetime,
    jsonpayload.extra.device_id,
    jsonpayload.extra.analyticssource,
    jsonpayload.extra.consent.mandatory as cookies_consent_mandatory,
    jsonpayload.extra.consent.accepted as cookies_consent_accepted,
    jsonpayload.extra.consent.refused as cookies_consent_refused,
    array_to_string(
        jsonpayload.extra.newlysubscribedto.themes, ' - '
    ) as newly_subscribed_themes,
    cast(jsonpayload.extra.newlysubscribedto.email as string) as newly_subscribed_email,
    cast(jsonpayload.extra.newlysubscribedto.push as string) as newly_subscribed_push,
    array_to_string(
        jsonpayload.extra.subscriptions.subscribed_themes, ' - '
    ) as currently_subscribed_themes,
    cast(
        jsonpayload.extra.subscriptions.marketing_push as string
    ) as currently_subscribed_marketing_push,
    cast(
        jsonpayload.extra.subscriptions.marketing_email as string
    ) as currently_subscribed_marketing_email,
    array_to_string(
        jsonpayload.extra.newlyunsubscribedfrom.themes, ' - '
    ) as newly_unsubscribed_themes,
    cast(
        jsonpayload.extra.newlyunsubscribedfrom.email as string
    ) as newly_unsubscribed_email,
    cast(
        jsonpayload.extra.newlyunsubscribedfrom.push as string
    ) as newly_unsubscribed_push
from {{ source("raw", "stdout") }}
where
    1 = 1
    {% if is_incremental() %} and date(timestamp) = "{{ ds() }}" {% endif %}
    and jsonpayload.extra.analyticssource = "app-native"
