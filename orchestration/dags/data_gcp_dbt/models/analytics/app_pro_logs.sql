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
    jsonpayload.user_id,
    jsonpayload.message,
    jsonpayload.technical_message_id,
    jsonpayload.extra.choice_datetime,
    jsonpayload.extra.device_id,
    jsonpayload.extra.analyticssource,
    jsonpayload.extra.consent.mandatory as cookies_consent_mandatory,
    jsonpayload.extra.consent.accepted as cookies_consent_accepted,
    jsonpayload.extra.consent.refused as cookies_consent_refused
from
    {{ source("raw","stdout") }}
where
    1 = 1
    {% if is_incremental() %}
        and DATE(timestamp) = "{{ ds }}"
    {% endif %}
    and jsonpayload.extra.analyticssource = "app-pro"
