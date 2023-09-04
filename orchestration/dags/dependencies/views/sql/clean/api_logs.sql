SELECT
    resource.labels.namespace_name as environement,
    CAST(jsonPayload.user_id as INT64) as user_id,
    CASE jsonPayload.extra.path
        WHEN "/native/v1/me" THEN "app_native"
        WHEN "/beneficiaries/current" THEN "webapp"
        WHEN "/users/current" THEN "pro"
    END AS source,
    CAST(jsonPayload.extra.statuscode as INT64) as status_code,
    jsonPayload.extra.deviceid as device_id,
    jsonPayload.extra.sourceip as source_ip,
    jsonPayload.extra.appversion as app_version,
    jsonPayload.extra.platform,
    timestamp,
FROM
    `{{ bigquery_raw_dataset }}.stdout`
