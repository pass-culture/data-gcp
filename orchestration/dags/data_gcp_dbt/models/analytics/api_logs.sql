{{
  config(materialized='view')
}}

select
    resource.labels.namespace_name as environement,
    CAST(jsonpayload.user_id as INT64) as user_id,
    jsonpayload.extra.path as url_path,
    case jsonpayload.extra.path
        when "/native/v1/me" then "app_native"
        when "/beneficiaries/current" then "webapp"
        when "/users/current" then "pro"
    end as source,
    CAST(jsonpayload.extra.statuscode as INT64) as status_code,
    jsonpayload.extra.deviceid as device_id,
    jsonpayload.extra.sourceip as source_ip,
    jsonpayload.extra.appversion as app_version,
    jsonpayload.extra.platform,
    timestamp,
    trace
from {{ source("raw","stdout") }}
where
    DATE(timestamp) >= DATE_SUB(CURRENT_DATE, interval 365 day)
    and jsonpayload.extra.path in ("/users/current", "/native/v1/me", "/native/v1/signin")
