SELECT
    DATE(timestamp) as partition_date,
    timestamp,
    COALESCE(CAST(jsonPayload.user_id AS STRING),CAST(jsonPayload.extra.user_id AS STRING)) AS user_id,
    jsonPayload.technical_message_id,
    jsonPayload.extra.choice_datetime,
    jsonPayload.extra.device_id,
    jsonPayload.extra.analyticssource,
    jsonPayload.extra.consent.mandatory as cookies_consent_mandatory,
    jsonPayload.extra.consent.accepted as cookies_consent_accepted,
    jsonPayload.extra.consent.refused as cookies_consent_refused,
    ARRAY_TO_STRING(jsonPayload.extra.newlysubscribedto.themes, ' - ') AS newly_subscribed_themes,
    CAST(jsonPayload.extra.newlysubscribedto.email AS STRING) AS newly_subscribed_email,
    CAST(jsonPayload.extra.newlysubscribedto.push AS STRING) AS newly_subscribed_push,
    ARRAY_TO_STRING(jsonPayload.extra.subscriptions.subscribed_themes, ' - ') AS currently_subscribed_themes,
    CAST(jsonPayload.extra.subscriptions.marketing_push AS STRING) AS currently_subscribed_marketing_push,
    CAST(jsonPayload.extra.subscriptions.marketing_email AS STRING) AS currently_subscribed_marketing_email,
    ARRAY_TO_STRING(jsonPayload.extra.newlyunsubscribedfrom.themes, ' - ')  AS newly_unsubscribed_themes,
    CAST(jsonPayload.extra.newlyunsubscribedfrom.email AS STRING) AS newly_unsubscribed_email,
    CAST(jsonPayload.extra.newlyunsubscribedfrom.push AS STRING) AS newly_unsubscribed_push,
    FROM
    `{{ bigquery_raw_dataset }}.stdout`
WHERE
    DATE(timestamp) = "{{ ds }}"
    AND jsonPayload.extra.analyticssource = "app-native"
