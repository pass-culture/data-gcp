select
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
    case
        url_path
        when "/native/v1/me"
        then "app_native"
        when "/beneficiaries/current"
        then "webapp"
        when "/users/current"
        then "pro"
    end as source
from {{ ref("int_pcapi__log") }}
where
    log_timestamp >= date_sub(current_timestamp(), interval 365 day)
    and url_path in ("/users/current", "/native/v1/me", "/native/v1/signin")
