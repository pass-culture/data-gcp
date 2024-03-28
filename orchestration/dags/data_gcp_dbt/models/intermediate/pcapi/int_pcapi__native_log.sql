SELECT
    log_timestamp,
    environement,
    user_id,
    url_path,
    status_code,
    device_id,
    source_ip,
    app_version,
    platform,
    trace,
    CASE url_path
        WHEN "/native/v1/me" THEN "app_native"
        WHEN "/beneficiaries/current" THEN "webapp"
        WHEN "/users/current" THEN "pro"
    END AS source,
FROM {{ref('int_pcapi__log')}}
WHERE log_timestamp >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 day)
AND url_path IN ("/users/current", "/native/v1/me", "/native/v1/signin")
