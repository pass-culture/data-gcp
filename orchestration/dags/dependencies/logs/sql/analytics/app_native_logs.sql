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
    jsonPayload.extra.consent.refused as cookies_consent_refused
FROM
    `{{ bigquery_raw_dataset }}.stdout`
WHERE
    DATE(timestamp) = "{{ ds }}"
    AND jsonPayload.extra.analyticssource = "app-native"
